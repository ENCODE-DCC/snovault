import datetime

from dateutil import parser

from snovault.elasticsearch.indexer import (
    INDEX_ENDPOINT_INFO,
    INDEX_EVENT_TAGS,
    IndexerInfo,
)


hash_key = 'fakehashkey'
item_key = 'fakeitemkey'
list_key = 'fakelistkey'


class TestIndexerInfo():

    def test_init(self):
        indexer_info = IndexerInfo()
        initial_state = indexer_info.hash_get(INDEX_ENDPOINT_INFO)
        assert initial_state == {}

    def test_update_indexer_info(self):
        indexer_info = IndexerInfo()
        indexer_info.update_indexer_info(indexer_info.state_waiting[0], {})
        state_hash = indexer_info.hash_get(INDEX_ENDPOINT_INFO)
        state_keys = list(state_hash.keys())
        state_keys.sort()
        class_state_keys = list(indexer_info.state_hash_keys)
        class_state_keys.sort()
        assert state_keys == class_state_keys
        assert state_hash['state'] == indexer_info.state_waiting[0]
        assert state_hash['state_desc'] == indexer_info.state_waiting[1]
        for key, val in state_hash.items():
            if key not in ['state', 'state_desc']:
                assert val == 'empty'
    
        indexer_info = IndexerInfo()
        update = {
            'pg_ip': '127.0.0.1',
            'remote_indexing': 'true',
            'loop_time': '27',
            'not_a_key': 'nothing',
        }
        indexer_info.update_indexer_info(indexer_info.state_endpoint_start[0], update)
        state_hash = indexer_info.hash_get(INDEX_ENDPOINT_INFO)
        assert state_hash['pg_ip'] == update['pg_ip']
        assert state_hash['remote_indexing'] == update['remote_indexing']
        assert state_hash['loop_time'] == update['loop_time']
        assert not 'not_a_key' in state_hash

    def test_get_indexing_events(self):
        indexer_info = IndexerInfo()
        events = indexer_info.get_indexing_events(0, -1)
        assert events == []

        indexer_info = IndexerInfo()
        events_to_add = 5
        start_tag = 10
        max_range = start_tag + events_to_add + 1
        for i in range(start_tag, max_range):
            local_tag = indexer_info.get_tag(num_bytes=4)
            start_time = i - 1
            len_invalidated = i
            indexer_info.list_add(INDEX_EVENT_TAGS, local_tag)
            indexer_info.item_set(f"{local_tag}:invalidated", str(len_invalidated))
            end_time = max_range
            indexer_info.item_set(f"{local_tag}:end",  'datetime string')
            indexer_info.item_set(f"{local_tag}:duration",  f"{end_time - start_time:.6f}")
        events = indexer_info.get_indexing_events(0, -1)
        assert len(events) == start_tag - events_to_add + 1
        for event in events:
            assert isinstance(event, str)
