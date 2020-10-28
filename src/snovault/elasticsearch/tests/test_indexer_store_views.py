import datetime
import pytest
import time

from unittest.mock import Mock, patch

from snovault.elasticsearch.interfaces import (
    INDEXER,
    INDEXER_EVENTS_LIST,
    INDEXER_EVENTS_TAG,
    INDEXER_STATE_TAG,
)
from snovault.elasticsearch.local_indexer_store import (
    IndexerStore,
    _EVENT_TAG_LEN,
)

def _setup_indexer_store(settings):
    app_settings['config_name'] = INDEXER
    return IndexerStore(app_settings)


class TestLocalStoreNoInitViews():
    '''
    No init.  IndexerStore with 'app' configuration.
    Calling the views prior to IndexerStore 'indexer' configuration initializing
    '''

    @pytest.fixture(autouse=True)
    def setup_class(self, testapp, app_settings):
        self.testapp = testapp
        # Delete previous redis keys
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
    
    # Event Views
    def test_indexer_store_events_tag(self):
        some_tag = 'a'*(2*_EVENT_TAG_LEN)
        event_tag = f"{INDEXER_EVENTS_TAG}:{some_tag}"
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        assert 'events_cnt' not in result
        assert 'events' not in result
        assert 'event' in result
        assert result['event'] == {}

    def test_indexer_store_events_bad_tag(self):
        event_tag = 'not-a-tag'
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []

        some_tag = 'a'*(_EVENT_TAG_LEN)
        event_tag = f"{INDEXER_EVENTS_TAG}:{some_tag}"
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []

    def test_indexer_store_events_range(self):
        url = '/indexer_store?events=3:4'
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' not in result
        assert 'events' in result
        assert result['events'] == []

    def test_indexer_store_events(self):
        url = '/indexer_store?events'
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []

    # State Views
    def test_indexer_store_state_only(self):
        url = '/indexer_store?state=state'
        result = self.testapp.get(url, status=200).json
        assert 'state' in result
        assert result['state'] == 'not initialized'

    def test_indexer_store_state_full(self):
        url = '/indexer_store?state=full'
        result = self.testapp.get(url, status=200).json
        assert result == {}

    def test_indexer_store_state(self):
        url = '/indexer_store?state'
        result = self.testapp.get(url, status=200).json
        assert result['early_return'] == 'state_obj state keys is Falsey'
        assert result['state_obj'] == self.local_store.get_state()


class TestLocalStoreInitViews():
    '''
    Init.  IndexerStore with 'indexer' configuration.
    Calling the views prior to IndexerStore 'indexer' configuration being set in the index endpoint
    '''
    
    @pytest.fixture(autouse=True)
    def setup_class(self, testapp, app_settings):
        self.testapp = testapp
        # Delete previous redis keys
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)

    @pytest.fixture(autouse=True)
    def setup_method(self, app_settings):
        # Delete previous redis keys
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
        # Create new local store
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)

    # Event Views
    def test_indexer_store_events_tag(self):
        some_tag = 'a'*(2*_EVENT_TAG_LEN)
        event_tag = f"{INDEXER_EVENTS_TAG}:{some_tag}"
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        print(result)
        assert 'events_cnt' not in result
        assert 'events' not in result
        assert 'event' in result
        assert result['event'] == {}

    def test_indexer_store_events_bad_tag(self):
        event_tag = 'not-a-tag'
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []

        some_tag = 'a'*(_EVENT_TAG_LEN)
        event_tag = f"{INDEXER_EVENTS_TAG}:{some_tag}"
        url = f"/indexer_store?events={event_tag}"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []

    def test_indexer_store_events_range(self):
        url = '/indexer_store?events=3:4'
        result = self.testapp.get(url, status=200).json
        print(result)
        assert 'event' not in result
        assert 'events_cnt' not in result
        assert 'events' in result
        assert result['events'] == []

    def test_indexer_store_events(self):
        url = '/indexer_store?events'
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 0
        assert 'events' in result
        assert result['events'] == []
    
    # State Views
    def test_indexer_store_state_only(self):
        url = '/indexer_store?state=state'
        result = self.testapp.get(url, status=200).json
        assert 'state' in result
        assert result['state'] == IndexerStore.state_initialized[0]

    def test_indexer_store_state_full(self):
        url = '/indexer_store?state=full'
        result = self.testapp.get(url, status=200).json
        assert result == self.local_store.get_state()

    def test_indexer_store_state(self):
        url = '/indexer_store?state'
        result = self.testapp.get(url, status=200).json
        assert result['early_return'] == 'state_obj state key is in intial state'
        assert result['state_obj'] == self.local_store.get_state()


class TestLocalStoreStateViews():
    '''
    IndexerStore with 'indexer' configuration.
    - IndexerStore endpoint has been called
    '''
    
    @pytest.fixture(autouse=True)
    def setup_class(self, testapp, app_settings):
        self.testapp = testapp
        # Delete previous redis keys
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
   
    @pytest.fixture(autouse=True)
    def setup_method(self, app_settings):
        # Delete previous redis keys
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            print(key)
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
        # Create new local store
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)

    # State Views
    @pytest.mark.parametrize(
        'state',
        (
            IndexerStore.state_endpoint_start,
            IndexerStore.state_load_indexing,
            IndexerStore.state_waiting,
        )
    )
    def test_indexer_store_state_no_event(self, state):
        self.local_store.set_state(state)
        url = '/indexer_store'
        result = self.testapp.get(url, status=200).json
        # Static
        assert 'static' in result
        assert isinstance(result['static'], dict)
        res_keys = list(result['static'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.static_keys
        # Dynamic
        assert 'dynamic' in result
        assert isinstance(result['dynamic'], dict)
        res_keys = list(result['dynamic'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.dynamic_keys
        # Event
        assert 'previous_event' in result
        assert isinstance(result['previous_event'], dict)
        res_keys = list(result['previous_event'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.event_keys
        for key in IndexerStore.event_keys:
            assert result['previous_event'][key] == 'unknown'

    @pytest.mark.parametrize(
        'state',
        (
            IndexerStore.state_endpoint_start,
            IndexerStore.state_load_indexing,
            IndexerStore.state_waiting,
        )
    )
    def test_indexer_store_state_prev_event(self, state):
        # Start run event
        time_mock = Mock(wraps=time.time)
        time_start = 38
        time_mock.return_value = time_start
        invalidated_cnt=83
        event_tag = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                IndexerStore.state_run_indexing,
                invalidated_cnt=invalidated_cnt
            )
        # End run event
        time_end = 48
        time_mock.return_value = time_end
        errors_cnt = 81
        with patch('time.time', new=time_mock):
            datetime_mock = Mock(wraps=datetime.datetime)
            datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
            with patch('datetime.datetime', new=datetime_mock):
                _, event_tag = self.local_store.set_state(
                    IndexerStore.state_run_indexing,
                    errors_cnt=errors_cnt,
                    event_tag=event_tag,
                )
        # Test in other states
        self.local_store.set_state(state)
        url = '/indexer_store'
        result = self.testapp.get(url, status=200).json
        # Static
        assert 'static' in result
        assert isinstance(result['static'], dict)
        res_keys = list(result['static'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.static_keys
        # Dynamic
        assert 'dynamic' in result
        assert isinstance(result['dynamic'], dict)
        res_keys = list(result['dynamic'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.dynamic_keys
        # Event
        assert 'previous_event' in result
        assert isinstance(result['previous_event'], dict)
        res_keys = list(result['previous_event'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.event_keys
        assert result['previous_event']['duration'] == str(time_end - time_start)
        assert result['previous_event']['end_time'] == str(datetime.datetime(1999, 1, 1))
        assert result['previous_event']['errors_cnt'] == str(errors_cnt)
        assert result['previous_event']['event_tag'] == event_tag
        assert result['previous_event']['invalidated_cnt'] == str(invalidated_cnt)
        assert result['previous_event']['start_ts'] == str(time_start)

    def test_indexer_store_state_curr_event(self):
        state, _ = self.local_store.set_state(IndexerStore.state_endpoint_start)
        self.local_store.set_state(IndexerStore.state_load_indexing)
        # Start run event
        time_mock = Mock(wraps=time.time)
        time_start = 38
        time_mock.return_value = time_start
        invalidated_cnt=83
        event_tag = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                IndexerStore.state_run_indexing,
                invalidated_cnt=invalidated_cnt
            )
        # Test endpoint
        url = '/indexer_store'
        time_mock = Mock(wraps=time.time)
        time_now = 49
        time_mock.return_value = time_now
        result = None
        with patch('time.time', new=time_mock):
            result = self.testapp.get(url, status=200).json
        # Static
        assert 'static' in result
        assert isinstance(result['static'], dict)
        res_keys = list(result['static'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.static_keys
        # Dynamic
        assert 'dynamic' in result
        assert isinstance(result['dynamic'], dict)
        res_keys = list(result['dynamic'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.dynamic_keys
        # Event
        assert 'current_event' in result
        assert isinstance(result['current_event'], dict)
        assert 'current_duration' in result
        duration = time_now - time_start
        assert result['current_duration'] == f"{duration:0.2f} seconds"
        res_keys = list(result['current_event'].keys())
        res_keys.sort()
        assert res_keys == IndexerStore.event_keys
        assert result['current_event']['duration'] == 'unknown'
        assert result['current_event']['end_time'] == 'unknown'
        assert result['current_event']['errors_cnt'] == 'unknown'
        assert result['current_event']['event_tag'] == event_tag
        assert result['current_event']['invalidated_cnt'] == str(invalidated_cnt)
        assert result['current_event']['start_ts'] == str(time_start)


    def test_indexer_store_state_curr_event_minutes(self):
        state, _ = self.local_store.set_state(IndexerStore.state_endpoint_start)
        self.local_store.set_state(IndexerStore.state_load_indexing)
        # Start run event
        time_mock = Mock(wraps=time.time)
        time_start = 10
        time_mock.return_value = time_start
        invalidated_cnt=38
        event_tag = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                IndexerStore.state_run_indexing,
                invalidated_cnt=invalidated_cnt
            )
        # Test endpoint
        url = '/indexer_store'
        time_mock = Mock(wraps=time.time)
        time_now = 130
        time_mock.return_value = time_now
        result = None
        with patch('time.time', new=time_mock):
            result = self.testapp.get(url, status=200).json
        # Current Event
        assert 'current_event' in result
        assert isinstance(result['current_event'], dict)
        assert 'current_duration' in result
        duration = (time_now - time_start)/60.0
        assert result['current_duration'] == f"{duration:0.2f} minutes"


    def test_indexer_store_state_curr_event_hours(self):
        state, _ = self.local_store.set_state(IndexerStore.state_endpoint_start)
        self.local_store.set_state(IndexerStore.state_load_indexing)
        # Start run event
        time_mock = Mock(wraps=time.time)
        time_start = 120
        time_mock.return_value = time_start
        invalidated_cnt=38
        event_tag = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                IndexerStore.state_run_indexing,
                invalidated_cnt=invalidated_cnt
            )
        # Test endpoint
        url = '/indexer_store'
        time_mock = Mock(wraps=time.time)
        time_now = 7320
        time_mock.return_value = time_now
        result = None
        with patch('time.time', new=time_mock):
            result = self.testapp.get(url, status=200).json
        # Current Event
        assert 'current_event' in result
        assert isinstance(result['current_event'], dict)
        assert 'current_duration' in result
        duration = (time_now - time_start)/3600.0
        assert result['current_duration'] == f"{duration:0.2f} hours"


class TestLocalStoreEventViews():

    def _add_event(self, event_number):
        event_dict = {
            'error_cnt': event_number*10,
            'invalidated_cnt': event_number*100,
            'time_start': event_number*1000,
            'time_end': event_number*10000,
        }
        self.local_store.set_state(IndexerStore.state_endpoint_start)
        self.local_store.set_state(IndexerStore.state_load_indexing)
        # Start run event
        time_mock = Mock(wraps=time.time)
        time_mock.return_value = event_dict['time_start']
        event_tag = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                IndexerStore.state_run_indexing,
                invalidated_cnt=event_dict['invalidated_cnt']
            )
            event_dict['event_tag'] = event_tag
        # End run event
        time_mock.return_value = event_dict['time_end']
        with patch('time.time', new=time_mock):
            datetime_mock = Mock(wraps=datetime.datetime)
            datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
            with patch('datetime.datetime', new=datetime_mock):
                self.local_store.set_state(
                    IndexerStore.state_run_indexing,
                    errors_cnt=event_dict['error_cnt'],
                    event_tag=event_dict['event_tag'],
                )
        self.local_store.set_state(IndexerStore.state_waiting)
        expected_result = {}
        event_dict['duration'] = event_dict['time_end'] - event_dict['time_start']
        expected_result[f"{event_tag}:duration"] = str(event_dict['duration'])
        expected_result[f"{event_tag}:invalidated_cnt"] = str(event_dict['invalidated_cnt'])
        expected_result[f"{event_tag}:end"] = str(datetime.datetime(1999, 1, 1))
        return event_dict, expected_result

    @pytest.fixture(autouse=True)
    def setup_class(self, testapp, app_settings):
        self.testapp = testapp
        # Delete previous redis keys
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)

    @pytest.fixture(autouse=True)
    def setup_method(self, app_settings):
        # Delete previous redis keys
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)
        for key in self.local_store.get_tag_keys(INDEXER_EVENTS_TAG):
            print(key)
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
        # Create new local store
        app_settings['config_name'] = INDEXER
        self.local_store = IndexerStore(app_settings)

    def test_indexer_store_events_one(self):
        event_dict_one, expected_result_one = self._add_event(1)
        # Test event
        url = f"/indexer_store?events={event_dict_one['event_tag']}"
        result = self.testapp.get(url, status=200).json
        assert 'events_cnt' not in result
        assert 'events' not in result
        assert 'event' in result
        assert result['event'] == expected_result_one
        # Test Range
        url = f"/indexer_store?events=0:1"
        result = self.testapp.get(url, status=200).json
        expected_result_str_one = f"{event_dict_one['event_tag']}: Indexed '{event_dict_one['invalidated_cnt']}' "
        expected_result_str_one += f"uuids in '{event_dict_one['duration']}' seconds. Ended at '1999-01-01 00:00:00'"
        assert 'event' not in result
        assert 'events_cnt' not in result
        assert 'events' in result
        assert len(result['events']) == 1
        assert result['events'][0] == expected_result_str_one
        # Test Tags
        url = f"/indexer_store?events"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 1
        assert 'events' in result
        assert len(result['events']) == 1
        assert result['events'][0] == event_dict_one['event_tag']

    def test_indexer_store_events_three(self):
        event_dict_one, expected_result_one = self._add_event(1)
        event_dict_two, expected_result_two = self._add_event(2)
        event_dict_three, expected_result_three = self._add_event(3)
        # Test event
        url = f"/indexer_store?events={event_dict_two['event_tag']}"
        result = self.testapp.get(url, status=200).json
        assert 'events_cnt' not in result
        assert 'events' not in result
        assert 'event' in result
        assert result['event'] == expected_result_two
        # Test Range
        url = f"/indexer_store?events=1:-1"
        result = self.testapp.get(url, status=200).json
        expected_result_str_one = f"{event_dict_one['event_tag']}: Indexed '{event_dict_one['invalidated_cnt']}' "
        expected_result_str_one += f"uuids in '{event_dict_one['duration']}' seconds. Ended at '1999-01-01 00:00:00'"
        expected_result_str_two = f"{event_dict_two['event_tag']}: Indexed '{event_dict_two['invalidated_cnt']}' "
        expected_result_str_two += f"uuids in '{event_dict_two['duration']}' seconds. Ended at '1999-01-01 00:00:00'"
        assert 'event' not in result
        assert 'events_cnt' not in result
        assert 'events' in result
        assert len(result['events']) == 2
        for ev in result['events']:
            print(ev)
        assert result['events'][0] == expected_result_str_two
        assert result['events'][1] == expected_result_str_one
        # Test Tags
        url = f"/indexer_store?events"
        result = self.testapp.get(url, status=200).json
        assert 'event' not in result
        assert 'events_cnt' in result
        assert result['events_cnt'] == 3
        assert 'events' in result
        assert len(result['events']) == 3
        assert result['events'][0] == event_dict_three['event_tag']
        assert result['events'][1] == event_dict_two['event_tag']
        assert result['events'][2] == event_dict_one['event_tag']
