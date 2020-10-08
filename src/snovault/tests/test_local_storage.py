import pytest

from snovault.local_storage import LocalStoreClient


def test_local_storage_server(redis_server, app_settings):
    local_store = LocalStoreClient(
        db_index=app_settings['local_storage_db'],
        host=app_settings['local_storage_host'],
        port=app_settings['local_storage_port'],
        timeout=app_settings['local_storage_timeout'],
    )
    try:
        local_store.client.ping()
    except Exception as excp:  # pylint: disable=broad-except
        print(excp)
        assert False
    assert True


class TestLocalStore():
    dict_key = 'fakedictkey'
    item_key = 'fakeitemkey'
    list_key = 'fakelistkey'
    local_store = None

    @pytest.fixture(autouse=True)
    def setup_method(self, redis_server, app_settings):
        '''
        Add local store to test class and cleans up standard redis keys
        - Uses the exposed redis client directly
        '''
        self.local_store = LocalStoreClient(
            db=app_settings['local_storage_db'],
            host=app_settings['local_storage_host'],
            port=app_settings['local_storage_port'],
            timeout=app_settings['local_storage_timeout'],
        ) 
        self.local_store.client.delete(self.dict_key)
        self.local_store.client.delete(self.item_key)
        for item in self.local_store.client.lrange(self.list_key, 0, -1):
            self.local_store.client.delete(item)
        self.local_store.client.delete(self.list_key)

    def test_init(self):
        try:
            self.local_store.ping()
        except Exception as excp:  # pylint: disable=broad-except
            assert False
        assert True

    def test_get_tag(self):
        tag = self.local_store.get_tag()
        assert isinstance(tag, str)
        assert len(tag) == 16
        tag = self.local_store.get_tag(num_bytes=4)
        assert len(tag) == 8

    def test_dict_get(self):
        hash_dict = self.local_store.dict_get(self.dict_key)
        assert isinstance(hash_dict, dict)
        assert hash_dict == {}

    def test_dict_set(self):
        update_dict = {'somekey': 'somevalue'}
        self.local_store.dict_set(self.dict_key, update_dict)
        hash_dict = self.local_store.dict_get(self.dict_key)
        assert hash_dict == update_dict

    def test_item_get(self):
        item = self.local_store.item_get(self.item_key)
        assert item is None

    def test_item_set(self):
        set_item = 'someitem'
        self.local_store.item_set(self.item_key, set_item)
        item = self.local_store.item_get(self.item_key)
        assert isinstance(item, str)
        assert item == set_item

    def test_list_add(self):
        # Lists are LIFO
        items_to_add = ['A', 'B', 'C']
        for item in items_to_add:
            self.local_store.list_add(self.list_key, item)
        items = self.local_store.list_get(self.list_key, 0, -1)
        items.sort()
        assert items_to_add == items

    def test_list_get(self):
        # Lists are LIFO
        items_to_add = ['A', 'B', 'C', 'D', 'E']
        for item in items_to_add:
            self.local_store.list_add(self.list_key, item)
        items = self.local_store.list_get(self.list_key, 0, 0)
        assert items[0] == items_to_add[-1]
        items = self.local_store.list_get(self.list_key, 1, 3)
        items_to_add.sort(reverse=True)
        assert items == items_to_add[1:4]
