def find_uuids_for_indexing(registry, updated, renamed, log):
    from .interfaces import ELASTIC_SEARCH
    from .create_mapping import index_settings
    from elasticsearch.exceptions import ConnectionTimeout
    es = registry[ELASTIC_SEARCH]
    SEARCH_MAX = 99999  # OutOfMemoryError if too high
    """
    Run a search to find uuids of objects with embedded uuids in updated
    or linked uuids in renamed. Only runs over the given indices.
    """
    invalidated = set()
    referencing = set()
    flush = False

    # if meta does not exist (shouldn't ever happen on deploy)
    # invalidate all uuids to avoid errors
    meta_exists = es.indices.exists(index='meta')
    if not meta_exists or len(updated) > SEARCH_MAX:
        referencing = set(get_uuids_for_types(registry))
        return invalidated, referencing, True

    es.indices.refresh(index='_all')
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
                                {
                                    'terms': {
                                        'linked_uuids': list(renamed),
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
    except ConnectionTimeout:
        # on timeout, queue everything for reindexing to avoid errors
        referencing = set(get_uuids_for_types(registry))
        return invalidated, referencing, True
    else:
        log.debug("Found %s associated items for indexing" %
                 (str(res['hits']['total'])))
        if res['hits']['total'] > SEARCH_MAX:
            referencing = set(get_uuids_for_types(registry))
            flush = True
        else:
            found_uuids = {hit['_id'] for hit in res['hits']['hits']}
            referencing = referencing | found_uuids
        invalidated = referencing | updated
        return invalidated, referencing, flush


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


def get_uuid_store_from_es(es):
    try:
        record = es.get(index='meta', doc_type='meta', id='uuid_store', ignore=[404])
    except:
        return None
    else:
        uuids = record.get('_source', {}).get('uuids', None)
        if uuids:
            # remove the record
            try:
                es.delete(index='meta', doc_type='meta', id='uuid_store', refresh=True)
            except:
                # delete failed, return no uuids for now
                return None
            else:
                return uuids
        else:
            return None
