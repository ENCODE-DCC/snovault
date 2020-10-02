from snovault.elasticsearch import LocalStoreClient


hash_key = 'fakehashkey'
item_key = 'fakeitemkey'
list_key = 'fakelistkey'


class TestLocalStoreClient():
    local_store = None

    def test_init(self):
        client = LocalStoreClient(local_store=self.local_store)
        assert hasattr(client, 'state')
        assert hasattr(client, 'events')
        assert hasattr(client, 'items')
        assert client.client is None

    def test_get_tag(self):
        client = LocalStoreClient(local_store=self.local_store)
        tag = client.get_tag()
        assert isinstance(tag, str)
        assert len(tag) == 16
        tag = client.get_tag(num_bytes=4)
        assert len(tag) == 8

    def test_hash_get(self):
        client = LocalStoreClient(local_store=self.local_store)
        hash_dict = client.hash_get(hash_key)
        assert isinstance(hash_dict, dict)
        assert hash_dict == {}

    def test_hash_set(self):
        client = LocalStoreClient(local_store=self.local_store)
        update_dict = {'somekey': 'somevalue'}
        client.hash_set(hash_key, update_dict)
        hash_dict = client.hash_get(hash_key)
        assert hash_dict == update_dict

    def test_item_get(self):
        client = LocalStoreClient(local_store=self.local_store)
        item = client.item_get(item_key)
        assert item is None

    def test_item_set(self):
        client = LocalStoreClient(local_store=self.local_store)
        set_item = 'someitem'
        client.item_set(item_key, set_item)
        item = client.item_get(item_key)
        assert isinstance(item, str)
        assert item == set_item

    def test_list_add(self):
        # Lists are LIFO
        client = LocalStoreClient(local_store=self.local_store)
        items_to_add = ['A', 'B', 'C']
        for item in items_to_add:
            client.list_add(list_key, item)
        items = client.list_get(list_key, 0, -1)
        items.sort()
        assert items_to_add == items

    def test_list_get(self):
        # Lists are LIFO
        client = LocalStoreClient(local_store=self.local_store)
        items_to_add = ['A', 'B', 'C', 'D', 'E']
        for item in items_to_add:
            client.list_add(list_key, item)
        items = client.list_get(list_key, 0, 0)
        assert items[0] == items_to_add[-1]
        items = client.list_get(list_key, 1, 3)
        items_to_add.sort(reverse=True)
        assert items == items_to_add[1:3]


class TestLocalStoreClientRedis(TestLocalStoreClient):
    local_store = 'redis'

    def setup_method(self):
        client = LocalStoreClient(local_store=self.local_store)
        for item in client.client.lrange(list_key, 0, -1):
            client.client.delete(item)
        client.client.delete(list_key)

    def teardown_class(self):
        client = LocalStoreClient(local_store=self.local_store)
        client.client.delete(hash_key)
        client.client.delete(item_key)
        for item in client.client.lrange(list_key, 0, -1):
            client.client.delete(item)
        client.client.delete(list_key)

    def test_init(self):
        client = LocalStoreClient(local_store=self.local_store)
        assert not hasattr(client, 'state')
        assert not hasattr(client, 'events')
        assert not hasattr(client, 'items')
        assert not client.client is None

    def test_list_add(self):
        # Lists are LIFO
        client = LocalStoreClient(local_store=self.local_store)
        items_to_add = ['A', 'B', 'C']
        for item in items_to_add:
            client.list_add(list_key, item)
        items = client.list_get(list_key, 0, -1)
        items.sort()
        assert items_to_add == items

    def test_list_get(self):
        # Lists are LIFO
        client = LocalStoreClient(local_store=self.local_store)
        print(client.client.lrange(list_key, 0, -1))
        items_to_add = ['A', 'B', 'C', 'D', 'E']
        for item in items_to_add:
            client.list_add(list_key, item)
        items = client.list_get(list_key, 0, 0)
        assert items[0] == items_to_add[-1]
        items = client.list_get(list_key, 1, 3)
        items_to_add.sort(reverse=True)
        assert items == items_to_add[1:4]
