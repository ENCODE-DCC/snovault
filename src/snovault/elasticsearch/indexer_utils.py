import time

def find_uuids_for_indexing(registry, updated, log=None):
    from .interfaces import ELASTIC_SEARCH
    from .create_mapping import index_settings
    from elasticsearch.exceptions import ConnectionTimeout
    es = registry[ELASTIC_SEARCH]
    SEARCH_MAX = 99999  # OutOfMemoryError if too high
    """
    Run a search to find uuids of objects with that contain the given updated
    uuids either in their embedded_uuids.
    Returns a set containing original uuids and the found uuids.
    If something goes wrong with the search or it's too large, return a set
    of all uuids.
    """
    invalidated = set()
    if len(updated) > SEARCH_MAX:  # use all uuids
        invalidated = set(get_uuids_for_types(registry))
        return invalidated | updated
    for backoff in [0, 1, 2, 4, 6]:  # arbitrary times
        time.sleep(backoff)
        try:
            res = es.search(index='_all', size=SEARCH_MAX, body={
                'query': {
                    'bool': {
                        'filter': {
                            'bool': {
                                'should': [
                                    {
                                        'terms': {
                                            'embedded_uuids': list(updated),
                                            '_cache': False,
                                        },
                                    },
                                ],
                            },
                        },
                    },
                },
                '_source': False,
            })
        except Exception as e:
            if log:
                log.warning('Retrying due to error with find_uuids_for_indexing: %s' % str(e))
            es.indices.refresh(index='_all')
        else:
            if log:
                log.debug("Found %s associated items for indexing" %
                        (str(res['hits']['total'])))
            if res['hits']['total'] > SEARCH_MAX:  # use all uuids
                invalidated = set(get_uuids_for_types(registry))
            else:
                invalidated = {hit['_id'] for hit in res['hits']['hits']}
            return invalidated | updated
    # this is only hit if the retry loop is exited. invalidate all
    invalidated = set(get_uuids_for_types(registry))
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


def get_xmin_from_es(es):
    from elasticsearch.exceptions import NotFoundError
    try:
        status = es.get(index='meta', doc_type='meta', id='indexing')
    except NotFoundError:
        interval_settings = {"index": {"refresh_interval": "30s"}}
        es.indices.put_settings(index='meta', body=interval_settings)
        return None
    else:
        return status['_source']['xmin']


def index_timestamp():
    """
    Returns an int datetime.utcnow() to microsecond resolution
    """
    return int(time.time() * 1000000)
