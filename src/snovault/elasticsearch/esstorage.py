from snovault.util import get_root_request
from elasticsearch.helpers import scan
from pyramid.threadlocal import get_current_request
from zope.interface import alsoProvides
from .interfaces import (
    ELASTIC_SEARCH,
    ICachedItem,
    RESOURCES_INDEX,
)


SEARCH_MAX = (2 ** 31) - 1


def includeme(config):
    from snovault import STORAGE
    registry = config.registry
    es = registry[ELASTIC_SEARCH]
    es_index = RESOURCES_INDEX
    wrapped_storage = registry[STORAGE]
    registry[STORAGE] = PickStorage(ElasticSearchStorage(es, es_index), wrapped_storage)


def force_database_for_request():
    request = get_current_request()
    if request:
        request.datastore = 'database'


class CachedModel(object):
    def __init__(self, hit):
        self.hit = hit
        self.source = hit['_source']

    @property
    def item_type(self):
        return self.source['item_type']

    @property
    def properties(self):
        return self.source['properties']

    @property
    def propsheets(self):
        return self.source['propsheets']

    @property
    def uuid(self):
        return self.source['uuid']

    @property
    def tid(self):
        return self.source['tid']

    def invalidated(self):
        request = get_root_request()
        if request is None:
            return False
        edits = dict.get(request.session, 'edits', None)
        if edits is None:
            return False
        version = self.hit['_version']
        source = self.source
        linked_uuids = set(source['linked_uuids'])
        embedded_uuids = set(source['embedded_uuids'])
        for xid, updated, linked in edits:
            if xid < version:
                continue
            if not embedded_uuids.isdisjoint(updated):
                return True
            if not linked_uuids.isdisjoint(linked):
                return True
        return False

    def used_for(self, item):
        alsoProvides(item, ICachedItem)


class PickStorage(object):
    def __init__(self, read, write):
        self.read = read
        self.write = write

    def storage(self):
        request = get_current_request()
        if request and request.datastore == 'elasticsearch':
            return self.read
        return self.write

    def get_by_uuid(self, uuid):
        storage = self.storage()
        model = storage.get_by_uuid(uuid)
        if storage is self.read:
            if model is None or model.invalidated():
                force_database_for_request()
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name, index=None):
        storage = self.storage()
        model = storage.get_by_unique_key(unique_key, name, index=index)
        if storage is self.read:
            if model is None or model.invalidated():
                force_database_for_request()
                return self.write.get_by_unique_key(unique_key, name, index=index)
        return model

    def get_rev_links(self, model, rel, *item_types):
        storage = self.storage()
        if isinstance(model, CachedModel) and storage is self.write:
            model = storage.get_by_uuid(str(model.uuid))
        return storage.get_rev_links(model, rel, *item_types)

    def __iter__(self, *item_types):
        return self.storage().__iter__(*item_types)

    def __len__(self, *item_types):
        return self.storage().__len__(*item_types)

    def create(self, item_type, uuid):
        return self.write.create(item_type, uuid)

    def update(self, model, properties=None, sheets=None, unique_keys=None, links=None):
        return self.write.update(model, properties, sheets, unique_keys, links)


class ElasticSearchStorage(object):
    writeable = False

    def __init__(self, es, index):
        self.es = es
        self.index = index

    def _one(self, query, index=None):
        if index is None:
            index = self.index
        data = self.es.search(index=index, body=query)
        hits = data['hits']['hits']
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0])
        return model

    def get_by_uuid(self, uuid):
        query = {
            'query': {
                'term': {
                    'uuid': str(uuid)
                }
            },
            'version': True
        }
        result = self.es.search(index=self.index, body=query, _source=True, size=1)
        if result['hits']['total'] == 0:
            return None
        hit = result['hits']['hits'][0]
        return CachedModel(hit)

    def get_by_unique_key(self, unique_key, name, index=None):
        term = 'unique_keys.' + unique_key
        query = {
            'query': {
                'term': {term: name}
            },
            'version': True,
        }
        return self._one(query, index)

    def get_rev_links(self, model, rel, *item_types):
        filter_ = {'term': {'links.' + rel: str(model.uuid)}}
        if item_types:
            filter_ = [
                filter_,
                {'terms': {'item_type': item_types}},
            ]
        query = {
            'stored_fields': [],
            'query': {
                'bool': {
                    'filter': filter_,
                }
            }
        }

        return [
            hit['_id'] for hit in scan(self.es, query=query)
        ]

    def __iter__(self, *item_types):
        query = {
            'stored_fields': [],
            'query': {
                'bool': {
                    'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
                }
            }
        }
        for hit in scan(self.es, query=query):
            yield hit['_id']

    def __len__(self, *item_types):
        query = {
            'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}},
        }
        result = self.es.count(index=self.index, body=query)
        return result['count']
