import datetime
import pytest
import time

from copy import copy
from unittest.mock import Mock, patch

from snovault.local_storage import LocalStoreClient

from snovault.elasticsearch.local_indexer_store import (
    IndexerStore,
    _EVENT_TAG_LEN,
)
from snovault.elasticsearch.interfaces import (
    INDEXER,
    INDEXER_EVENTS_LIST,
    INDEXER_EVENTS_TAG,
    INDEXER_STATE_TAG,
)



def _get_local_app_settings(settings):
    local_settings = copy(settings)
    local_settings['config_name'] = 'app'
    return local_settings


def _get_local_indexer_settings(local_settings):
    local_settings.update(_get_local_app_settings(local_settings))
    local_settings['config_name'] = INDEXER
    local_settings['pg_ip'] = '1.2.3.4'
    local_settings['remote_indexing'] = 'false'
    local_settings['timeout'] = '10'
    return local_settings


def test_init_app_config(app_settings):
    local_settings = _get_local_app_settings(app_settings)
    indexer_store = IndexerStore(local_settings)
    assert not hasattr(indexer_store, 'pg_ip')
    assert not hasattr(indexer_store, 'remote_indexing')
    assert not hasattr(indexer_store, 'loop_time')
    assert indexer_store.get_state() == {}


def test_init_indexer_config(app_settings):
    datetime_mock = Mock(wraps=datetime.datetime)
    datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
    with patch('datetime.datetime', new=datetime_mock):
        local_settings = _get_local_indexer_settings(app_settings)
        indexer_store = IndexerStore(local_settings)
        assert hasattr(indexer_store, 'pg_ip')
        assert hasattr(indexer_store, 'remote_indexing')
        assert hasattr(indexer_store, 'loop_time')
        state_hash = indexer_store.get_state()
        # Keys
        state_hash_keys = list(state_hash.keys())
        state_hash_keys.sort()
        all_keys = list(IndexerStore.static_keys)
        all_keys.extend(list(IndexerStore.dynamic_keys))
        all_keys.extend(list(IndexerStore.event_keys))
        all_keys.sort()
        assert state_hash_keys == all_keys
        # Static values
        assert state_hash['addr'] == f"{INDEXER}:{id(indexer_store)}"
        assert state_hash['config_name'] == INDEXER
        assert state_hash['init_dt'] == str(datetime.datetime(1999, 1, 1))
        assert state_hash['loop_time'] == '10'
        assert state_hash['pg_ip'] == '1.2.3.4'
        assert state_hash['remote_indexing'] == 'false'
        assert state_hash['state_key'] == INDEXER_STATE_TAG
        # Dynamic
        assert state_hash['endpoint_end'] == 'unknown'
        assert state_hash['endpoint_start'] == 'unknown'
        assert state_hash['state'] == indexer_store.state_initialized[0]
        assert state_hash['state_error'] == 'unknown'
        assert state_hash['state_desc'] == indexer_store.state_initialized[1]
        # Event
        for key in IndexerStore.event_keys:
            assert state_hash[key] == 'unknown'


class TestLocalStore():
    fake_event_tag = 'fakeeventtag'

    @pytest.fixture(autouse=True)
    def setup_method(self, app_settings):
        '''
        Add local store to test class and cleans up standard redis keys
        - Uses the exposed redis client directly
        '''
        # Delete previous redis keys
        local_settings = _get_local_indexer_settings(copy(app_settings))
        self.local_store = IndexerStore(local_settings)
        for key in self.local_store.get_tag_keys(self.fake_event_tag):
            self.local_store.client.delete(key)
        self.local_store.client.delete(INDEXER_EVENTS_LIST)
        self.local_store.client.delete(INDEXER_STATE_TAG)
        # Create new local store
        local_settings = _get_local_indexer_settings(copy(app_settings))
        self.local_store = IndexerStore(local_settings)

    def test_end_event(self):
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
        with patch('datetime.datetime', new=datetime_mock):
            duration = '4'
            self.local_store._end_event(self.fake_event_tag, duration)
            event_duration_tag = f"{self.fake_event_tag}:duration"
            assert self.local_store.client.get(event_duration_tag) == duration
            event_end_tag = f"{self.fake_event_tag}:end"
            assert self.local_store.client.get(event_end_tag), str(datetime.datetime(1999, 1, 1))

    def test_start_event(self):
        invalidated_cnt = '4'
        self.local_store._start_event(self.fake_event_tag, invalidated_cnt)
        event_invalidated_cnt_tag = f"{self.fake_event_tag}:invalidated_cnt"
        assert self.local_store.client.get(event_invalidated_cnt_tag) == invalidated_cnt
        all_keys = self.local_store.client.lrange(INDEXER_EVENTS_LIST, 0, -1)
        assert len(all_keys) == 1
        assert all_keys[0] == self.fake_event_tag

    def test_get_event(self):
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
        with patch('datetime.datetime', new=datetime_mock):
            invalidated_cnt = '14'
            self.local_store._start_event(self.fake_event_tag, invalidated_cnt)
            duration = '2'
            self.local_store._end_event(self.fake_event_tag, duration)
            event_str = self.local_store.get_event(self.fake_event_tag)
            expected_event_str = f"Indexed '{invalidated_cnt}' uuids in '{duration}' seconds."
            expected_event_str = f"{expected_event_str} Ended at '{datetime.datetime.utcnow()}'"
            expected_event_str = f"{self.fake_event_tag}: {expected_event_str}"
            assert event_str == expected_event_str

    def test_get_state(self):
        test_dict = {'test': 'testing'}
        self.local_store.client.delete(INDEXER_STATE_TAG)
        self.local_store.client.hmset(INDEXER_STATE_TAG, test_dict)
        assert self.local_store.get_state() == test_dict

    def test_set_state_endpoint_start(self):
        original_state = self.local_store.get_state()
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
        with patch('datetime.datetime', new=datetime_mock):
            self.local_store.set_state(self.local_store.state_endpoint_start)
            new_state = self.local_store.get_state()
            for key, val in original_state.items():
                if key not in IndexerStore.dynamic_keys:
                    assert val == new_state[key]
            assert new_state['endpoint_end'] == 'tbd'
            assert new_state['endpoint_start'] == str(datetime.datetime(1999, 1, 1))
            assert new_state['state_error'] == 'tbd'
            assert new_state['state'] == self.local_store.state_endpoint_start[0]
            assert new_state['state_desc'] == self.local_store.state_endpoint_start[1]

    def test_set_state_load_indexing(self):
        original_state = self.local_store.get_state()
        self.local_store.set_state(self.local_store.state_load_indexing)
        new_state = self.local_store.get_state()
        for key, val in original_state.items():
            if key not in ['state', 'state_desc']:
                assert val == new_state[key]
        assert new_state['state'] == self.local_store.state_load_indexing[0]
        assert new_state['state_desc'] == self.local_store.state_load_indexing[1]

    def test_set_state_run_indexing_start(self):
        invalidated_cnt = 9
        original_state = self.local_store.get_state()
        time_mock = Mock(wraps=time.time)
        time_start = time.time()
        time_mock.return_value = time_start
        new_event_tag = None
        new_state = None
        with patch('time.time', new=time_mock):
            new_state, new_event_tag = self.local_store.set_state(
                self.local_store.state_run_indexing,
                invalidated_cnt=invalidated_cnt,
            )
            for key in IndexerStore.static_keys:
                assert original_state[key] == new_state[key]
            
            assert new_state['endpoint_end'] == original_state['endpoint_end']
            assert new_state['endpoint_start'] == original_state['endpoint_start']
            assert new_state['state_error'] == original_state['state_error']
            assert new_state['state'] == self.local_store.state_run_indexing[0]
            assert new_state['state_desc'] == self.local_store.state_run_indexing[1]
            
            assert new_state['end_time'] == original_state['end_time']
            assert new_state['errors_cnt'] == original_state['errors_cnt']
            some_tag = self.local_store.get_tag(INDEXER_EVENTS_TAG, num_bytes=_EVENT_TAG_LEN)
            assert len(new_state['event_tag']) == len(some_tag)
            assert new_state['event_tag'] == new_event_tag
            assert new_state['duration'] == original_state['duration']
            assert new_state['invalidated_cnt'] == str(invalidated_cnt)
            assert new_state['start_ts'] == str(int(time_start))

    def test_set_state_run_indexing_end(self):
        # start event
        invalidated_cnt = 11
        time_mock = Mock(wraps=time.time)
        time_start = 30 
        time_mock.return_value = time_start
        event_tag = None
        original_state = None
        with patch('time.time', new=time_mock):
            original_state, event_tag = self.local_store.set_state(
                self.local_store.state_run_indexing,
                invalidated_cnt=invalidated_cnt,
            )
        # end event
        errors_cnt = 3
        time_mock = Mock(wraps=time.time)
        time_end= 40
        time_mock.return_value = time_end
        new_event_tag = None
        new_state = None
        with patch('time.time', new=time_mock):
            new_state, new_event_tag = self.local_store.set_state(
                self.local_store.state_run_indexing,
                errors_cnt=errors_cnt,
                event_tag=original_state['event_tag'],
            )
            for key in IndexerStore.static_keys:
                assert original_state[key] == new_state[key]
            for key in IndexerStore.dynamic_keys:
                assert original_state[key] == new_state[key]
            
            assert isinstance(new_state['end_time'], str)
            assert new_state['errors_cnt'] == str(errors_cnt)
            assert new_state['event_tag'] == event_tag
            assert new_state['duration'] == str(time_end - time_start)
            assert new_state['invalidated_cnt'] == original_state['invalidated_cnt']
            assert new_state['start_ts'] == original_state['start_ts']

    def test_set_state_waiting(self):
        # Start endpoint
        original_state, _ = self.local_store.set_state(self.local_store.state_endpoint_start)
        # Load indexing
        datetime_mock = Mock(wraps=datetime.datetime)
        datetime_mock.utcnow.return_value = datetime.datetime(1999, 1, 1)
        with patch('datetime.datetime', new=datetime_mock):
            self.local_store.set_state(self.local_store.state_load_indexing)
        # Start Event
        time_mock = Mock(wraps=time.time)
        time_start = int(500)
        time_mock.return_value = time_start
        event_tag = None
        original_state = None
        with patch('time.time', new=time_mock):
            _, event_tag = self.local_store.set_state(
                self.local_store.state_run_indexing,
                invalidated_cnt=7,
            )
        # End Event
        time_end = int(700)
        time_mock.return_value = time_end
        with patch('time.time', new=time_mock):
            self.local_store.set_state(self.local_store.state_run_indexing, event_tag=event_tag)
        # Endpoint End
        new_state, _ = self.local_store.set_state(self.local_store.state_waiting)
        # Make sure the last event exists during waiting state
        assert new_state['event_tag'] == event_tag
        assert new_state['duration'] == str(time_end - time_start)
        # Check state
        assert new_state['state'] == self.local_store.state_waiting[0]
        assert new_state['state_desc'] == self.local_store.state_waiting[1]

    def test_set_state_run_indexing_no_args(self):
        run_state = self.local_store.state_run_indexing
        original_state, event_tag = self.local_store.set_state(self.local_store.state_run_indexing)
        assert event_tag is None
        assert original_state['state_error'] == f"{str(run_state)} requries additional arguments"

    def test_set_state_error_type(self):
        bad_state = 'not a tuple'
        expected_state_error = f"state, '{str(bad_state)}', is not a tuple"
        state, _ = self.local_store.set_state(bad_state)
        assert state['state_error'] == expected_state_error

        bad_state = IndexerStore
        expected_state_error = f"state, '{str(bad_state)}', is not a tuple"
        state, _ = self.local_store.set_state(bad_state)
        assert state['state_error'] == expected_state_error

    def test_set_state_error_len(self):
        bad_state = tuple()
        expected_state_error = f"state, {bad_state}, is wrong length"
        state, _ = self.local_store.set_state(bad_state)
        assert state['state_error'] == expected_state_error

        bad_state = tuple(['a'])
        expected_state_error = f"state, {bad_state}, is wrong length"
        state, _ = self.local_store.set_state(bad_state)
        assert state['state_error'] == expected_state_error

        bad_state = tuple(['a', 'b', 'c'])
        expected_state_error = f"state, {bad_state}, is wrong length"
        state, _ = self.local_store.set_state(bad_state)
        assert state['state_error'] == expected_state_error

    def test_set_state_error_dne(self):
        not_allow_state_change = tuple(['state_init', 'initialized'])
        expected_state_error = f"{str(not_allow_state_change)} is not a valid state change"
        state, _ = self.local_store.set_state(not_allow_state_change)
        assert state['state_error'] == expected_state_error

        dne_state = tuple(['state_dne', 'does not exist'])
        expected_state_error = f"{str(dne_state)} is not a valid state change"
        state, _ = self.local_store.set_state(dne_state)
        assert state['state_error'] == expected_state_error
