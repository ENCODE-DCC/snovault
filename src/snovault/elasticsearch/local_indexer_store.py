import datetime
import re
import time

from pyramid.view import view_config

from snovault.local_storage import (
    LocalStoreClient,
    base_result,
)
from snovault.elasticsearch.interfaces import (
    INDEXER,
    INDEXER_STORE,
    INDEXER_STATE_TAG,
    INDEXER_EVENTS_TAG,
    INDEXER_EVENTS_LIST,
)


_EVENT_TAG_LEN = 4
_EVENT_PATTERN = re.compile("^" + INDEXER_EVENTS_TAG + ":[a-z0-9]{" + str(2*_EVENT_TAG_LEN) + "}$")
_EVENTS_PATTERN = re.compile("^-?\d+:-?\d+$")
_EVENTS_DEFAULT_RANGE = '0:100'


def includeme(config):
    config.scan(__name__)
    config.add_route('indexer_store', '/indexer_store')
    config.registry[INDEXER_STORE] = IndexerStore(config.registry.settings)



@view_config(route_name='indexer_store', request_method='GET', request_param='events')
def indexer_store_events(request):
    '''
    Get indexer event(s) based on regex of events value
        * This redis list is LIFO
    
    1. Value looks like an event tag:
        * indexer_event:4fc592b0 
    2. Value looks like a range:
        * All   events=0:-1
        * First events=-1:-1
        * Last/Most recent events=0:0
        * Range recent events=4:94
    3. Default no events value uses _EVENTS_DEFAULT_RANGE
    '''
    request_argument = request.params.get("events")
    if not request_argument:
        # Test for falsey, request params returns empty string if not events value given
        request_argument = _EVENTS_DEFAULT_RANGE
    indexer_store = request.registry[INDEXER_STORE]
    result = base_result(indexer_store)
    if _EVENT_PATTERN.match(request_argument):
        # request argument looks like an event_tag
        result['event'] = {}
        for event_key in indexer_store.get_tag_keys(request_argument):
            result['event'][event_key] = indexer_store.item_get(event_key)
    elif _EVENTS_PATTERN.match(request_argument):
        # request argument looks like range string
        colon = request_argument.index(':')
        start = int(request_argument[:colon])
        stop = int(request_argument[colon+1:])
        event_tags = indexer_store.list_get(INDEXER_EVENTS_LIST, start, stop)
        result['events'] = []
        for event_tag in event_tags:
            result['events'].append(indexer_store.get_event_msg(event_tag))
    else:
        # Events arg is bad format?
        result['error'] = f"Bad events value '{request_argument}'"
        result['error_desc'] = 'Must be key(indexer_event:4fc592b0) or range(4:94) like'
    request.query_string = "format=json"
    return result


@view_config(route_name='indexer_store', request_method='GET', request_param='state=raw')
def indexer_store_state_raw(request):
    '''Only return raw state object from redis'''
    request.query_string = "format=json"
    return request.registry[INDEXER_STORE].get_state()


@view_config(route_name='indexer_store', request_method='GET', request_param='state=state')
def indexer_store_state_split(request):
    '''All state info organized into sections with some extra info and error checking'''
    indexer_store = request.registry[INDEXER_STORE]
    state_obj = indexer_store.get_state()
    current_state = state_obj.get('state')
    # Early return with raw full state due lack of state or still initializing
    if not current_state:
        return {
            'early_return': 'state_obj state keys is Falsey',
            'state_obj': state_obj,
        }
    elif current_state == IndexerStore.state_initialized[0]:
        return {
            'early_return': 'state_obj state key is in intial state',
            'state_obj': state_obj,
        }
    # Normal return
    result = base_result(indexer_store)
    # Add Static Info 
    result['init'] = {}
    for key in IndexerStore.init_keys:
        result['init'][key] = state_obj[key]
    result['init']['up_time'] = indexer_store._duration_with_unis_str(state_obj['init_dt'])
    # Add Dynamic State
    result['dynamic'] = {}
    for key in IndexerStore.state_keys:
        result['dynamic'][key] = state_obj[key]
    result['dynamic']['time_in_state'] = indexer_store._duration_with_unis_str(state_obj['state_start_dt'])
    # Event Load
    if state_obj['load_end_dt'] == 'tbd':
        load_key = 'current_load'
        load_duration = indexer_store._duration_with_unis_str(state_obj['load_start_dt'])
    else:
        load_key = 'previous_load'
        load_duration = indexer_store._duration_with_unis_str(
            state_obj['load_start_dt'],
            end_dt=state_obj['load_end_dt'],
        )
    # Add current or previous run load keys
    result[load_key] = {}
    for key in IndexerStore.load_keys:
        result[load_key][key] = state_obj.get(key)
    result[load_key]['run_time'] = load_duration
    # Event
    if state_obj['event_end_dt'] == 'tbd':
        event_key = 'current_event'
        event_duration = indexer_store._duration_with_unis_str(state_obj['event_start_dt'])
    else:
        event_key = 'previous_event'
        event_duration = indexer_store._duration_with_unis_str(
            state_obj['event_start_dt'],
            end_dt=state_obj['event_end_dt'],
        )
    # Add current or previous run event keys
    result[event_key] = {}
    for key in indexer_store.event_keys:
        result[event_key][key] = state_obj.get(key)
    result[event_key]['run_time'] = event_duration
    request.query_string = "format=json"
    return result


@view_config(route_name='indexer_store', request_method='GET')
def indexer_store_state(request):
    '''Minimal view for current state and additional info if running'''
    indexer_store = request.registry[INDEXER_STORE]
    state_obj = indexer_store.get_state()
    current_state = state_obj.get('state', 'not initialized')
    result = {
        'state': current_state,
        'time_in_state': indexer_store._duration_with_unis_str(state_obj['state_start_dt']),
    }
    if current_state == IndexerStore.state_waiting[0]:
        result['description'] = f"Remains in state for {state_obj['loop_time']} seconds."
    elif current_state == IndexerStore.state_initialized[0]:
        result['time_since_init'] = indexer_store._duration_with_unis_str(state_obj['init_dt'])
        result['description'] = 'Very short duration.  Happens once during deployment'
    elif current_state == IndexerStore.state_endpoint_start[0]:
        result['time_in_state'] = indexer_store._duration_with_unis_str(state_obj['state_start_dt'])
        result['description'] = 'Very short duration.  Happens once a minute.'
    elif current_state == IndexerStore.state_load_indexing[0]:
        result['description'] = 'Time depends on number of uuids to index.  Could take minutes.'
    elif current_state == IndexerStore.state_run_indexing[0]:
        if state_obj['event_end_dt'] == 'tbd':
            result['time_since_event_start'] = indexer_store._duration_with_unis_str(state_obj['event_start_dt'])
            result['description'] = 'Time depends on number of uuids to index.  Could take hours.'
            result['current_event_tag'] = state_obj['event_tag']
            result['current_invalidated_cnt'] = state_obj['event_invalidated_cnt']
        else:
            result['time_since_event_end'] = indexer_store._duration_with_unis_str(state_obj['event_start_dt'])
            result['description'] = 'Short duration.  Should go to waiting soon.'
            result['just_finished_event_tag'] = state_obj.get('event_tag', 'not initialized')
            result['just_finished_invalidated_cnt'] = state_obj['event_invalidated_cnt']
    request.query_string = "format=json"
    return result



class IndexerStore(LocalStoreClient):
    config_name = INDEXER
    state_initialized = ('state_init', 'initialized')
    state_endpoint_start = ('state_endpoint_start', 'Endpoint started running')
    state_load_indexing = ('state_load_indexing', 'Endpoint checking for uuids to index')
    state_run_indexing = ('state_run_indexing', 'Endpoint found uuids and started indexing')
    state_waiting = ('state_waiting', 'Waiting to call endpoint')
    init_keys = [
        'addr',
        'config_name',
        'hash_key',
        'init_dt',
        'loop_time',
        'pg_ip',
        'remote_indexing',
    ]
    state_keys = [
        'state',
        'state_desc',
        'state_error',
        'state_start_dt',
    ]
    load_keys = [
        'load_end_dt',
        'load_start_dt',
        'priority_invalidated',
        'first_txn',
        'snapshot_id',
        'xmin',
        'last_xmin',
        'txn_count',
        'updated'
        'renamed',
        'related_set_total',
        'related_set',
    ]
    event_keys = [
        'event_end_dt',
        'event_errors_cnt',
        'event_tag',
        'event_invalidated_cnt',
        'event_start_dt',
    ]
    
    def __init__(self, settings):
        super().__init__(
            db_index=settings['local_storage_redis_index'],
            host=settings['local_storage_host'],
            port=settings['local_storage_port'],
            local_tz=settings['local_tz'],
            socket_timeout=int(settings['local_storage_timeout']),
        )
        if settings.get('config_name', 'no name') == IndexerStore.config_name:
            # only the indexer process should set these attributes and initialize this store 
            self.pg_ip = str(settings.get('pg_ip', 'localhost'))
            self.remote_indexing = str(settings.get('remote_indexing', 'false'))
            self.loop_time = str(settings.get('timeout', 'unknown'))
            curr_state = self.get_state()
            if not curr_state:
                self._init_state()

    @staticmethod
    def _dt_now():
        return str(datetime.datetime.utcnow())

    @staticmethod
    def _convert_dt_to_ts(dt_str):
        '''Expecting dt_str to be in the format 2020-10-23 17:09:36.442405'''
        try:
            return datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f').timestamp()
        except ValueError:
            return None

    def _duration_with_unis_str(self, start_dt, end_dt=None):
        '''
        Return duration in seconds, minutes, or hours
        - Input expected to be in '2020-10-23 17:09:36.442405' format
        - From now if end_dt not provided
        '''
        start_ts = self._convert_dt_to_ts(start_dt)
        if not start_ts:
            return f"Could not determine duration with start_dt={start_dt}"
        if not end_dt:
            end_dt = self._dt_now()
        end_ts = self._convert_dt_to_ts(end_dt)
        if not end_ts:
            return f"Could not determine duration with end_dt={end_dt}"
        seconds = float(end_ts - start_ts)
        div = 1.0
        unit = 'seconds'
        if seconds > 60:
            div = 60.0
            unit = 'minutes'
        if seconds > 3600:
            div = 3600.0
            unit = 'hours'
        return f"{seconds/div:0.2f} {unit}"

    def _init_state(self):
        '''Indexer state in redis is named after the process in the config'''
        init_state = {
            # Static
            'addr': f"{self.config_name}:{id(self)}",
            'config_name': self.config_name,
            'init_dt': str(datetime.datetime.utcnow()),
            'loop_time': str(self.loop_time),
            'pg_ip': self.pg_ip,
            'remote_indexing': self.remote_indexing,
            'hash_key': INDEXER_STATE_TAG,
        }
        # Dynamic
        for key in self.state_keys:
            init_state[key] = 'unknown'
        init_state['state'] = self.state_initialized[0]
        init_state['state_desc'] = self.state_initialized[1]
        # Load event
        for key in self.load_keys:
            init_state[key] = 'unknown'
        self.dict_set(INDEXER_STATE_TAG, init_state)
        # Run event
        for key in self.event_keys:
            init_state[key] = 'unknown'
        self.dict_set(INDEXER_STATE_TAG, init_state)

    def _end_event(self, event_tag, state):
        '''Close event with only certain event keys in state.  Also add human readable date time'''
        for event_key in ['event_end_dt', 'event_errors_cnt']:
            self.item_set(f"{event_tag}:{event_key}",  state[event_key])
        self.item_set(f"{event_tag}:end",  str(datetime.datetime.utcnow()))
    
    def _start_event(self, event_tag, state):
        print(state)
        '''Create new event with info from state in events keys'''
        self.list_add(INDEXER_EVENTS_LIST, event_tag)
        for event_key in self.event_keys:
            self.item_set(f"{event_tag}:{event_key}", state[event_key])

    def get_event_msg(self, event_tag):
        end_dt = str(self.item_get(event_tag + ':event_end_dt'))
        start_dt = str(self.item_get(event_tag + ':event_start_dt'))
        errors_cnt = str(self.item_get(event_tag + ':event_errors_cnt'))
        invalidated_cnt = str(self.item_get(event_tag + ':event_invalidated_cnt'))
        duration = self._duration_with_unis_str(start_dt, end_dt=end_dt)
        msg = (
            f"Indexed '{invalidated_cnt}' uuids in '{duration}'"
            f"with '{errors_cnt}' errors. Ended at '{end_dt}'."
        )
        return f"{event_tag}: {msg}"

    def get_state(self):
        return self.dict_get(INDEXER_STATE_TAG)

    def _set_state(self, state_tuple, new_state, start_dt=None):
        # Set valid state and store
        new_state['state'] = state_tuple[0]
        new_state['state_desc'] = state_tuple[1]
        if start_dt:
            new_state['state_start_dt'] = start_dt
        self.dict_set(INDEXER_STATE_TAG, new_state)

    def set_state(self, state_tuple, **kwargs):
        state = self.get_state()
        state['state_start_dt'] = 'tbd'
        state['state_error'] = ''
        if state_tuple[0] == self.state_waiting[0]:
            # Waiting in es_index_listener for timeout
            self._set_state(state_tuple, state, start_dt=self._dt_now())
            return self.get_state(), None
        elif state_tuple[0] == self.state_endpoint_start[0]:
            self._set_state(state_tuple, state, start_dt=self._dt_now())
            return self.get_state(), None
        elif state_tuple[0] == self.state_load_indexing[0] and 'priority_invalidated' in kwargs:
            # Start Loading
            for key in self.load_keys:
                state[key] = 'tbd'
            short_list = f"[{','.join(kwargs['priority_invalidated'])}]"[0:100]
            state['priority_invalidated'] = short_list
            state['load_start_dt'] = self._dt_now()
            self._set_state(state_tuple, state, start_dt=self._dt_now())
            return self.get_state(), None
        elif state_tuple[0] == self.state_load_indexing[0]:
            for key in self.load_keys:
                if kwargs.get(key):
                    if isinstance(kwargs[key], list):
                        kwargs[key] = f"[{','.join(kwargs[key])}]"
                    state[key] = kwargs[key]
            state['load_end_dt'] = self._dt_now()
            self._set_state(state_tuple, state, start_dt=self._dt_now())
            return self.get_state(), None
        elif state_tuple[0] == self.state_run_indexing[0] and kwargs.get('invalidated_cnt'):
            # Reset event keys
            for event_key in self.event_keys:
                state[event_key] = 'tbd'
            # Start indexing
            state['event_tag'] = self.get_tag(INDEXER_EVENTS_TAG, num_bytes=_EVENT_TAG_LEN)
            state['event_invalidated_cnt'] = str(kwargs['invalidated_cnt'])
            state['event_start_dt'] = self._dt_now()
            self._start_event(state['event_tag'], state)
            self._set_state(state_tuple, state, start_dt=self._dt_now())
            return self.get_state(), state['event_tag']
        elif state_tuple[0] == self.state_run_indexing[0] and kwargs.get('event_tag'):
            # End indexing
            state['event_end_dt'] = self._dt_now() 
            state['event_errors_cnt'] = str(kwargs.get('errors_cnt', 0))
            self._end_event(kwargs['event_tag'], state)
            self._set_state(state_tuple, state)
            return self.get_state(), kwargs['event_tag']
        elif state_tuple[0] == self.state_run_indexing[0]:
            # Invalid State
            state['state_error'] = f"{str(state_tuple)} requries additional arguments"
            self.dict_set(INDEXER_STATE_TAG, state)
            self._set_state(state_tuple, state)
            return self.get_state(), None
        else:
            # Invalid State
            state['state_error'] = f"{str(state_tuple)} is not a valid state change"
            self.dict_set(INDEXER_STATE_TAG, state)
            return self.get_state(), None
