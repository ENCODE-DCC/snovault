import elasticsearch.exceptions
from snovault.util import get_root_request
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match
from pyramid.threadlocal import get_current_request
from zope.interface import alsoProvides
from .interfaces import (
    ELASTIC_SEARCH,
    ICachedItem,
)

SEARCH_MAX = (2 ** 31) - 1


def includeme(config):
    from snovault import STORAGE
    registry = config.registry
    es = registry[ELASTIC_SEARCH]
    # ES 5 change: 'snovault' index removed, search among '_all' instead
    es_index = '_all'
    wrapped_storage = registry[STORAGE]
    registry[STORAGE] = PickStorage(ElasticSearchStorage(es, es_index), wrapped_storage)


class CachedModel(object):
    def __init__(self, hit):
        self.hit = hit.to_dict()
        self.meta = hit.meta.to_dict()

    @property
    def item_type(self):
        return self.hit['item_type']

    @property
    def properties(self):
        return self.hit['properties']

    @property
    def propsheets(self):
        return self.hit['propsheets']

    @property
    def uuid(self):
        return self.hit['uuid']

    @property
    def tid(self):
        return self.hit['tid']

    def invalidated(self):
        request = get_root_request()
        if request is None:
            return False
        edits = dict.get(request.session, 'edits', None)
        if edits is None:
            return False
        version = self.meta['version']
        linked_uuids = set(self.hit['linked_uuids'])
        embedded_uuids = set(self.hit['embedded_uuids'])
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
        hits = search.execute()
        if len(hits) != 1:
            return None
        model = CachedModel(hits[0])
        return model

    def get_by_uuid(self, uuid):
        try:
            hit = self.es.get(index=self.index, id=str(uuid))
        except elasticsearch.exceptions.NotFoundError:
            return None
        return CachedModel(hit)

    def get_by_json(self, key, value, item_type, default=None):
        # find the term with the specific type
        # also this query will do it to...
        # {‘fields’: [], ‘filter’: {‘and’: [{‘term’: {‘embedded.term_name.raw’: ‘lung’}}, {‘terms’:
        # {‘item_type’: [‘ontology_term’]}}]}, ‘_source’: [‘embedded’]}
        term = 'embedded.' + key + '.raw'

        query = {
            'filter': {'and': [
                {'term': {term: value}},
                {'type': {'value': item_type}},
            ]}
        }
        res = self._one(query)
        return res


    def get_by_unique_key(self, unique_key, name):
        term = 'unique_keys.' + unique_key
        # had to use ** kw notation because of variable in field name
        search = Search(using=self.es)
        search = search.filter('term', **{term: name})
        search = search.extra(version=True)
        return self._one(search)

    def get_rev_links(self, model, rel, *item_types):
        search = Search(using=self.es)
        search = search.params(size=SEARCH_MAX)
        # had to use ** kw notation because of variable in field name
        search = search.filter('term', **{'links.' + rel: str(model.uuid)})
        if item_types:
            search = search.filter('terms', item_type=item_types)
        hits = search.execute()
        return [
            hit.to_dict()['_id'] for hit in hits
        ]

    def __iter__(self, *item_types):
        query = {
            'fields': [],
            'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}},
        }
        for hit in scan(self.es, query=query):
            yield hit['_id']

    def __len__(self, *item_types):
        query = {
            'filter': {'terms': {'item_type': item_types}} if item_types else {'match_all': {}},
        }
        result = self.es.count(index=self.index, body=query)
        return result['count']
