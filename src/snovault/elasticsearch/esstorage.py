import elasticsearch.exceptions
from snovault.util import get_root_request
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search, Q
from pyramid.threadlocal import get_current_request
from pyramid.httpexceptions import (
    HTTPLocked
)
from zope.interface import alsoProvides
from .interfaces import (
    ELASTIC_SEARCH,
    ICachedItem,
)
from ..storage import RDBStorage
from .indexer_utils import find_uuids_for_indexing

SEARCH_MAX = 99999  # OutOfMemoryError if too high. Previously: (2 ** 31) - 1

def includeme(config):
    from snovault import STORAGE
    registry = config.registry
    es = registry[ELASTIC_SEARCH]
    # ES 5 change: 'snovault' index removed, search among '_all' instead
    es_index = '_all'
    wrapped_storage = registry[STORAGE]
    registry[STORAGE] = PickStorage(ElasticSearchStorage(es, es_index), wrapped_storage, registry)


class CachedModel(object):
    def __init__(self, hit):
        self.source = hit.to_dict()
        self.meta = hit.meta.to_dict()

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
        version = self.meta['version']
        linked_uuids = set(self.source['linked_uuids'])
        embedded_uuids = set(self.source['embedded_uuids'])
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
    def __init__(self, read, write, registry):
        self.read = read
        self.write = write
        self.registry = registry

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
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name):
        storage = self.storage()
        model = storage.get_by_unique_key(unique_key, name)
        if storage is self.read:
            if model is None or model.invalidated():
                return self.write.get_by_unique_key(unique_key, name)
        return model


    def get_by_json(self, key, value, item_type, default=None):
        storage = self.storage()
        model = storage.get_by_json(key, value, item_type)
        if storage is self.read:
            if model is None or model.invalidated():
                return self.write.get_by_json(key, value, item_type)
        return model

    def delete_by_uuid(self, rid, item_type=None):
        if not item_type: # ES deletion requires index & doc_type, which are both == item_type
            model = self.get_by_uuid(rid)
            item_type = model.item_type
        uuids_linking_to_item = find_uuids_for_indexing(self.registry, set(), set([rid]))[0] - set([rid])
        if len(uuids_linking_to_item) > 0:
            raise HTTPLocked(detail="Cannot delete an Item from the database if other Items are still linking to it.", comment=[
                # Return list of { '@id', 'display_title', 'uuid' } in 'comment' property in response to assist with any manual unlinking.
                { p:v for p,v in self.read.get_by_uuid(uuid).source.get('embedded', {}).items() if p in ('@id', 'display_title', 'uuid') } for uuid in uuids_linking_to_item
            ])

        self.write.delete_by_uuid(rid)              # Deletes from RDB
        self.read.delete_by_uuid(rid, item_type)    # Deletes from ES

    def get_rev_links(self, model, rel, *item_types):
        return self.storage().get_rev_links(model, rel, *item_types)

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

    def _one(self, search):
        # execute search and return a model if there is one hit
        hits = search.execute()
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0])
        return model

    def get_by_uuid(self, uuid):
        search = Search(using=self.es)
        id_query = Q('ids', values=[str(uuid)])
        search = search.query(id_query)
        return self._one(search)

    def get_by_json(self, key, value, item_type, default=None):
        # find the term with the specific type
        term = 'embedded.' + key + '.raw'
        search = Search(using=self.es)
        search = search.filter('term', **{term: value})
        search = search.filter('type', value=item_type)
        return self._one(search)


    def get_by_unique_key(self, unique_key, name):
        term = 'unique_keys.' + unique_key
        # had to use ** kw notation because of variable in field name
        search = Search(using=self.es)
        search = search.filter('term', **{term: name})
        search = search.extra(version=True)
        return self._one(search)

    def get_rev_links(self, model, rel, *item_types):
        search = Search(using=self.es)
        search = search.extra(size=SEARCH_MAX)
        # had to use ** kw notation because of variable in field name
        search = search.filter('term', **{'links.' + rel: str(model.uuid)})
        if item_types:
            search = search.filter('terms', item_type=item_types)
        hits = search.execute()
        return [hit.to_dict().get('uuid', hit.to_dict().get('_id')) for hit in hits]

    def delete_by_uuid(self, rid, item_type=None):
        if not item_type:
            model = self.get_by_uuid(rid)
            item_type = model.item_type
        self.es.delete(id=rid, index=item_type, doc_type=item_type)

    def __iter__(self, *item_types):
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        for hit in scan(self.es, query=query):
            yield hit.get('uuid', hit.get('_id'))

    def __len__(self, *item_types):
        query = {'query': {
            'bool': {
                'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}}
            }
        }}
        result = self.es.count(index=self.index, body=query)
        return result['count']
