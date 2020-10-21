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


def _duration_with_unis_str(ts_start):
    '''Return duration in seconds, minutes, or hours'''
    seconds = float(time.time()) - int(ts_start)
    div = 1.0
    unit = 'seconds'
    if seconds > 60:
        div = 60.0
        unit = 'minutes'
    if seconds > 3600:
        div = 3600.0
        unit = 'hours'
    return f"{seconds/div:0.2f} {unit}"


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
            result['events'].append(indexer_store.get_event(event_tag))
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


@view_config(route_name='indexer_store', request_method='GET', request_param='state=split')
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
    result['static'] = {}
    for key in IndexerStore.static_keys:
        result['static'][key] = state_obj[key]
    # Add Dynamic State
    result['dynamic'] = {}
    for key in IndexerStore.dynamic_keys:
        result['dynamic'][key] = state_obj[key]
    # Determine if run events and run state
    event_key = None
    if current_state in [
            IndexerStore.state_endpoint_start[0],
            IndexerStore.state_waiting[0],
            IndexerStore.state_load_indexing[0],
    ]:
        event_key = 'previous_event'
    elif current_state == IndexerStore.state_run_indexing[0]:
        event_key = 'previous_event'
        if state_obj['end_time'] == 'unknown':
            event_key = 'current_event'
    # Add current or previous run event keys
    if event_key:
        result[event_key] = {}
        for key in indexer_store.event_keys:
            result[event_key][key] = state_obj[key]
        # Add formatted duration and event_tag if running
        result[event_key]['formated_duration'] = _duration_with_unis_str(state_obj['start_ts'])
    request.query_string = "format=json"
    return result


@view_config(route_name='indexer_store', request_method='GET')
def indexer_store_state(request):
    '''Minimal view for current state and additional info if running'''
    request.query_string = "format=json"
    state_obj = request.registry[INDEXER_STORE].get_state()
    current_state = state_obj.get('state', 'not initialized')
    result = {
        'state': current_state,
    }
    if state_obj.get('state') in [IndexerStore.state_endpoint_start[0], IndexerStore.state_load_indexing[0]]:
        result['state_duration'] = _duration_with_unis_str(state_obj['endpoint_start_ts'])
    elif state_obj.get('state') == IndexerStore.state_run_indexing[0]:
        # Add formatted duration and event_tag if running
        result['current_event_tag'] = state_obj.get('event_tag', 'not initialized')
        result['current_duration'] = _duration_with_unis_str(state_obj['start_ts'])
        result['current_invalidated_cnt'] = state_obj['invalidated_cnt']
    request.query_string = "format=json"
    return result


class IndexerStore(LocalStoreClient):
    config_name = INDEXER
    state_initialized = ('state_init', 'initialized')
    state_endpoint_start = ('state_endpoint_start', 'Endpoint started running')
    state_load_indexing = ('state_load_indexing', 'Endpoint checking for uuids to index')
    state_run_indexing = ('state_run_indexing', 'Endpoint found uuids and started indexing')
    state_waiting = ('state_waiting', 'Waiting to call endpoint')
    static_keys = [
        'addr',
        'config_name',
        'init_dt',
        'loop_time',
        'pg_ip',
        'remote_indexing',
        'state_key',
    ]
    dynamic_keys = [
        'endpoint_end',
        'endpoint_start',
        'endpoint_start_ts',
        'state',
        'state_desc',
        'state_error',
    ]
    event_keys = [
        'duration',
        'end_time',
        'errors_cnt',
        'event_tag',
        'invalidated_cnt',
        'start_ts',
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
            'state_key': INDEXER_STATE_TAG,
        }
        # Dynamic
        for key in self.dynamic_keys:
            init_state[key] = 'unknown'
        init_state['state'] = self.state_initialized[0]
        init_state['state_desc'] = self.state_initialized[1]
        # Run event
        for key in self.event_keys:
            init_state[key] = 'unknown'
        self.dict_set(INDEXER_STATE_TAG, init_state)

    def _end_event(self, event_tag, state):
        '''Close event with only certain event keys in state.  Also add human readable date time'''
        for event_key in ['duration', 'end_time', 'errors_cnt']:
            self.item_set(f"{event_tag}:{event_key}",  state[event_key])
        self.item_set(f"{event_tag}:end",  str(datetime.datetime.utcnow()))
    
    def _start_event(self, event_tag, state):
        '''Create new event with info from state in events keys'''
        self.list_add(INDEXER_EVENTS_LIST, event_tag)
        for event_key in self.event_keys:
            self.item_set(f"{event_tag}:{event_key}", state[event_key])

    def get_event(self, event_tag):
        end = str(self.item_get(event_tag + ':end'))
        invalidated_cnt = str(self.item_get(event_tag + ':invalidated_cnt'))
        duration = str(self.item_get(event_tag + ':duration'))
        msg = f"Indexed '{invalidated_cnt}' uuids in '{duration}' seconds. Ended at '{end}'"
        return f"{event_tag}: {msg}"

    def get_state(self):
        return self.dict_get(INDEXER_STATE_TAG)

    def set_state(self, state_tuple, errors_cnt=0, invalidated_cnt=0, event_tag=None):
        state = self.get_state()
        if not isinstance(state_tuple, tuple):
            # Invalid State type
            state['state_error'] = f"state, '{str(state_tuple)}', is not a tuple"
            self.dict_set(INDEXER_STATE_TAG, state)
            return self.get_state(), None
        elif not len(state_tuple) == 2:
            # Invalid State len
            state['state_error'] = f"state, {state_tuple}, is wrong length"
            self.dict_set(INDEXER_STATE_TAG, state)
            return self.get_state(), None
        elif state_tuple[0] == self.state_endpoint_start[0]:
            # Reset
            state['endpoint_end'] = 'tbd'
            state['endpoint_start'] = str(datetime.datetime.utcnow())
            state['endpoint_start_ts'] = str(int(time.time()))
            state['state_error'] = 'tbd'
        elif state_tuple[0] == self.state_load_indexing[0]:
            # Checking for uuids to index
            pass
        elif state_tuple[0] == self.state_run_indexing[0] and invalidated_cnt:
            # Start indexing
            event_tag = self.get_tag(INDEXER_EVENTS_TAG, num_bytes=_EVENT_TAG_LEN)
            state['event_tag'] = event_tag
            state['invalidated_cnt'] = str(invalidated_cnt)
            state['start_ts'] = str(int(time.time()))
            self._start_event(state['event_tag'], state)
        elif state_tuple[0] == self.state_run_indexing[0] and event_tag:
            # End indexing
            state['end_time'] = str(datetime.datetime.utcnow()) 
            state['errors_cnt'] = str(errors_cnt)
            duration = int(time.time()) - int(state['start_ts'])
            state['duration'] = str(duration)
            self._end_event(event_tag, state)
        elif state_tuple[0] == self.state_waiting[0]:
            # Waiting in es_index_listener for timeout
            state['endpoint_end'] = str(datetime.datetime.utcnow())
        elif state_tuple[0] == self.state_run_indexing[0]:
            # Invalid State
            state['state_error'] = f"{str(state_tuple)} requries additional arguments"
            self.dict_set(INDEXER_STATE_TAG, state)
            return self.get_state(), None
        else:
            # Invalid State
            state['state_error'] = f"{str(state_tuple)} is not a valid state change"
            self.dict_set(INDEXER_STATE_TAG, state)
            return self.get_state(), None
        # Set valid state and store
        state['state'] = state_tuple[0]
        state['state_desc'] = state_tuple[1]
        self.dict_set(INDEXER_STATE_TAG, state)
        return self.get_state(), event_tag
