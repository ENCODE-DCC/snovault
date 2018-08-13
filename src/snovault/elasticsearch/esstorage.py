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


def find_linking_property(our_dict, value_to_find):
    """
    Helper function used in PickStorage.find_uuids_linked_to_item
    """
    def find_it(d, parent_key=None):
        if isinstance(d, list):
            for idx, v in enumerate(d):
                if isinstance(v, dict) or isinstance(v, list):
                    found = find_it(v, parent_key)
                    if found:
                        return (parent_key if parent_key else '') + '[' + str(idx) + '].' + found
                elif v == value_to_find:
                    return '[' + str(idx) + ']'
        elif isinstance(d, dict):
            for k, v in d.items():
                if isinstance(v, dict) or isinstance(v, list):
                    found = find_it(v, k)
                    if found:
                        return found
                elif v == value_to_find:
                    return k
        return None

    return find_it(our_dict)


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

    @property
    def sid(self):
        return self.source.get('sid')

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
            if model is None:
                return self.write.get_by_uuid(uuid)
        return model

    def get_by_unique_key(self, unique_key, name):
        storage = self.storage()
        model = storage.get_by_unique_key(unique_key, name)
        if storage is self.read:
            if model is None:
                return self.write.get_by_unique_key(unique_key, name)
        return model


    def get_by_json(self, key, value, item_type, default=None):
        storage = self.storage()
        model = storage.get_by_json(key, value, item_type)
        if storage is self.read:
            if model is None:
                return self.write.get_by_json(key, value, item_type)
        return model


    def find_uuids_linked_to_item(self, rid):
        """
        Given a resource id (rid), such as uuid, find all items in the DB
        that have a linkTo to that item.
        Returns some extra information about the fields/links that are
        present
        """
        linked_info = []
        # we only care about linkTos the item and not reverse links here
        uuids_linking_to_item = find_uuids_for_indexing(self.registry, set([rid]),
                                                        skip_revs=True)
        # remove the item itself from the list
        uuids_linking_to_item = uuids_linking_to_item - set([rid])
        if len(uuids_linking_to_item) > 0:
            # Return list of { '@id', 'display_title', 'uuid' } in 'comment'
            # property of HTTPException response to assist with any manual unlinking.
            for linking_uuid in uuids_linking_to_item:
                linking_dict = self.read.get_by_uuid(linking_uuid).source.get('embedded')
                linking_property = find_linking_property(linking_dict, rid)
                linked_info.append({
                    '@id' : linking_dict['@id'],
                    'display_title' : linking_dict['display_title'],
                    'uuid' : linking_uuid,
                    'field' : linking_property or "Not Embedded"
                })
        return linked_info


    def purge_uuid(self, rid, item_type=None):
        """
        Attempt to purge an item by given resource id (rid), completely
        removing it from ES and DB.
        """
        if not item_type: # ES deletion requires index & doc_type, which are both == item_type
            model = self.get_by_uuid(rid)
            item_type = model.item_type
        uuids_linking_to_item = self.find_uuids_linked_to_item(rid)
        if len(uuids_linking_to_item) > 0:
            raise HTTPLocked(detail="Cannot purge item as other items still link to it",
                             comment=uuids_linking_to_item)

        self.write.purge_uuid(rid)  # Deletes from RDB
        # queue related items for reindexing
        self.registry[INDEXER].find_and_queue_secondary_items(set(rid), set())
        try:
            self.read.purge_uuid(rid, item_type)  # Deletes from ES
        except elasticsearch.exceptions.NotFoundError:
            # Case: Not yet indexed
            print('Couldn\'t find ' + rid + ' in ElasticSearch. Continuing.')
        except elasticsearch.exceptions.UnknownItemTypeError:
            # Case: Deleting in-database item for collection which no longer exists. Probably temporary.
            print('Item type ' + str(item_type) + ' does not exist in ElasticSearch. Continuing.')

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

    def purge_uuid(self, rid, item_type=None):
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
