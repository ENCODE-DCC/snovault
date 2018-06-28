from .interfaces import ELASTIC_SEARCH
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import scan
import time


def find_uuids_for_indexing(registry, updated, log=None):
    """
    Run a search to find uuids of objects with that contain the given updated
    uuids either in their referenced_uuids.
    Uses elasticsearch.helpers.scan to iterate through ES results
    Returns a set containing original uuids and the found uuids (INCLUDING
    uuids that were passed into this function)
    """
    es = registry[ELASTIC_SEARCH]
    scan_query = {
        'query': {
            'bool': {
                'filter': {
                    'bool': {
                        'should': [
                            {
                                'terms': {
                                    'referenced_uuids': list(updated),
                                    '_cache': False,
                                },
                            },
                        ],
                    },
                },
            },
        },
        '_source': False,
    }
    results = scan(es, index='_all', query=scan_query)
    invalidated = {res['_id'] for res in results}
    return invalidated | updated


def get_uuids_for_types(registry, types=None):
    from snovault import COLLECTIONS
    """
    Generator function to return uuids for all the given types. If no
    types provided, uses all types (get all uuids).
    """
    # First index user and access_key so people can log in
    collections = registry[COLLECTIONS]
    initial = ['user', 'access_key']
    for collection_name in initial:
        collection = collections.by_item_type.get(collection_name, [])
        # for snovault test application, there are no users or keys
        if types is not None and collection_name not in types:
            continue
        for uuid in collection:
            yield str(uuid)
    for collection_name in sorted(collections.by_item_type):
        if collection_name in initial:
            continue
        if types is not None and collection_name not in types:
            continue
        collection = collections.by_item_type[collection_name]
        for uuid in collection:
            yield str(uuid)
