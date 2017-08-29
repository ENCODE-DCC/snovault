""" Test full indexing setup

The fixtures in this module setup a full system with postgresql and
elasticsearch running as subprocesses.
Does not include data dependent tests
"""

import pytest
import time
from snovault.elasticsearch.interfaces import ELASTIC_SEARCH
from snovault import (
    COLLECTIONS,
    TYPES,
)

pytestmark = [pytest.mark.indexing]


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture(scope='session')
def app_settings(wsgi_server_host_port, elasticsearch_server, postgresql_server):
    from .testappfixtures import _app_settings
    settings = _app_settings.copy()
    settings['create_tables'] = True
    settings['persona.audiences'] = 'http://%s:%s' % wsgi_server_host_port
    settings['elasticsearch.server'] = elasticsearch_server
    settings['sqlalchemy.url'] = postgresql_server
    settings['collection_datastore'] = 'elasticsearch'
    settings['item_datastore'] = 'elasticsearch'
    settings['indexer'] = True
    settings['indexer.processes'] = 2
    return settings


@pytest.yield_fixture(scope='session')
def app(app_settings):
    from snovault import main
    app = main({}, **app_settings)

    yield app

    # Shutdown multiprocessing pool to close db conns.
    from snovault.elasticsearch import INDEXER
    app.registry[INDEXER].shutdown()

    from snovault import DBSESSION
    DBSession = app.registry[DBSESSION]
    # Dispose connections so postgres can tear down.
    DBSession.bind.pool.dispose()


@pytest.fixture(scope='session')
def DBSession(app):
    from snovault import DBSESSION
    return app.registry[DBSESSION]


@pytest.fixture(autouse=True)
def teardown(app, dbapi_conn):
    from snovault.elasticsearch import create_mapping
    create_mapping.run(app)
    cursor = dbapi_conn.cursor()
    cursor.execute("""TRUNCATE resources, transactions CASCADE;""")
    cursor.close()


@pytest.yield_fixture
def dbapi_conn(DBSession):
    connection = DBSession.bind.pool.unique_connection()
    connection.detach()
    conn = connection.connection
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.yield_fixture
def listening_conn(dbapi_conn):
    cursor = dbapi_conn.cursor()
    cursor.execute("""LISTEN "snovault.transaction";""")
    yield dbapi_conn
    cursor.close()


def test_indexing_simple(app, testapp, indexer_testapp):
    from snovault.elasticsearch import indexer_utils
    # First post a single item so that subsequent indexing is incremental
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexed'] == 1
    assert 'txn_count' not in res.json

    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexed'] == 1
    assert res.json['txn_count'] == 1
    assert res.json['updated'] == [uuid]
    res = testapp.get('/search/?type=TestingPostPutPatch')
    uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
    count = 0
    while uuid not in uuids and count < 20:
        time.sleep(1)
        res = testapp.get('/search/?type=TestingPostPutPatch')
        uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
        count += 1
    assert res.json['total'] >= 2
    assert uuid in uuids
    # test the meta index
    es = app.registry[ELASTIC_SEARCH]
    indexing_doc = es.get(index='meta', doc_type='meta', id='indexing')
    indexing_source = indexing_doc['_source']
    utils_xmin = indexer_utils.get_xmin_from_es(es)
    assert 'xmin' in indexing_source
    assert 'last_xmin' in indexing_source
    assert 'indexed' in indexing_source
    assert indexing_source['xmin'] == utils_xmin
    assert indexing_source['xmin'] >= indexing_source['last_xmin']
    testing_ppp_meta = es.get(index='meta', doc_type='meta', id='testing_post_put_patch')
    testing_ppp_source = testing_ppp_meta['_source']
    assert 'mappings' in testing_ppp_source
    assert 'settings' in testing_ppp_source


def test_es_indices(app, elasticsearch):
    """
    Test overall create_mapping functionality using app.
    Do this by checking es directly before and after running mapping.
    Delete an index directly, run again to see if it recovers.
    """
    from snovault.elasticsearch.create_mapping import type_mapping, create_mapping_by_type, build_index_record
    es = app.registry[ELASTIC_SEARCH]
    item_types = app.registry[TYPES].by_item_type
    # check that mappings and settings are in index
    for item_type in item_types:
        item_mapping = type_mapping(app.registry[TYPES], item_type)
        try:
            item_index = es.indices.get(index=item_type)
        except:
            assert False
        found_index_mapping = item_index.get(item_type, {}).get('mappings').get(item_type, {}).get('properties', {}).get('embedded')
        found_index_settings = item_index.get(item_type, {}).get('settings')
        assert found_index_mapping
        assert found_index_settings
        # get the item record from meta and compare that
        full_mapping = create_mapping_by_type(item_type, app.registry)
        item_record = build_index_record(full_mapping, item_type)
        try:
            item_meta = es.get(index='meta', doc_type='meta', id=item_type)
        except:
            assert False
        meta_record = item_meta.get('_source', None)
        assert meta_record
        assert item_record == meta_record


def test_listening(testapp, listening_conn):
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(1)
    listening_conn.poll()
    assert len(listening_conn.notifies) == 1
    notify = listening_conn.notifies.pop()
    assert notify.channel == 'snovault.transaction'
    assert int(notify.payload) > 0


def test_index_settings(app, testapp, indexer_testapp):
    from snovault.elasticsearch.create_mapping import index_settings
    test_type = 'testing_post_put_patch'
    es_settings = index_settings(test_type)
    max_result_window = es_settings['index']['max_result_window']
    # preform some initial indexing to build meta
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    # need to make sure an xmin was generated for the following to work
    assert 'xmin' in res.json
    es = app.registry[ELASTIC_SEARCH]
    curr_settings = es.indices.get_settings(index=test_type)
    found_max_window = curr_settings.get(test_type, {}).get('settings', {}).get('index', {}).get('max_result_window', None)
    # test one important setting
    assert int(found_max_window) == max_result_window


def test_indexing_es(app, testapp, indexer_testapp):
    """
    Get es results directly and test to make sure the _embedded results
    match with the embedded list in the types files.
    """
    from snovault.elasticsearch import create_mapping, indexer_utils
    from elasticsearch.exceptions import NotFoundError
    import datetime
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    # no documents added yet
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 0
    # post a document but do not yet index
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 0
    # indexing record should not yet exist (expect error)
    with pytest.raises(NotFoundError):
        es.get(index='meta', doc_type='meta', id='indexing')
    res = indexer_testapp.post_json('/index', {'record': True})
    # let indexer do its thing
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 1
    indexing_record = es.get(index='meta', doc_type='meta', id='indexing')
    indexing_record_source = indexing_record.get('_source', {})
    assert indexing_record_source.get('indexed') == 1
    # test timing in indexing record
    assert indexing_record_source.get('indexing_elapsed')
    indexing_start = indexing_record_source.get('indexing_started')
    indexing_end = indexing_record_source.get('indexing_finished')
    assert indexing_start and indexing_end
    assert datetime.strptime(indexing_start) < datetime.strptime(indexing_end)
    # run create_mapping with check_first=False (do not expect a re-index)
    reindex_uuids = create_mapping.run(app)
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 0
    assert len(reindex_uuids) == 0
    with pytest.raises(NotFoundError):
        es.get(index='meta', doc_type='meta', id='indexing')
    # index to create the indexing doc
    res = indexer_testapp.post_json('/index', {'record': True})
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 1
    indexing_record = es.get(index='meta', doc_type='meta', id='indexing')
    assert indexing_record.get('_source', {}).get('indexed') == 1
    # delete index and re-run create_mapping
    # should queue the single uuid for indexing in stored_uuids
    es.indices.delete(index=test_type)
    reindex_uuids = create_mapping.run(app, check_first=True)
    time.sleep(3)
    store_uuids = indexer_utils.get_uuid_store_from_es(es)
    assert len(reindex_uuids) == 1
    assert reindex_uuids == set(store_uuids)
    # reindex with --force and --item-type
    reindex_uuids = create_mapping.run(app, collections=[test_type], force=True)
    time.sleep(3)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 1
    assert len(reindex_uuids) == 1
    # check indexing record for 'types_indexed'
    indexing_record = es.get(index='meta', doc_type='meta', id='indexing')
    assert indexing_record.get('_source', {}).get('indexed') == 1
    assert indexing_record.get('_source', {}).get('types_indexed') == [test_type]
    # post second item to database but do not index (don't load into es)
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    # doc_count has not yet updated
    assert doc_count == 1
    # run create mapping with force=True, expect test index to re-index
    reindex_uuids = create_mapping.run(app, force=True)
    assert len(reindex_uuids) == 2
    time.sleep(3)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    # doc_count will have updated due to indexing in create_mapping
    assert doc_count == 2
    res = indexer_testapp.post_json('/index', {'record': True})


# some unit tests associated with build_index in create_mapping
def test_check_if_index_exists(app):
    from snovault.elasticsearch.create_mapping import check_if_index_exists
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    exists = check_if_index_exists(es, test_type, True)
    assert exists
    # delete index
    es.indices.delete(index=test_type)
    exists = check_if_index_exists(es, test_type, True)
    assert not exists


def test_get_previous_index_record(app):
    from snovault.elasticsearch.create_mapping import get_previous_index_record
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    record = get_previous_index_record(True, True, es, test_type)
    assert record
    assert 'mappings' in record
    assert 'settings' in record
    # remove index record
    es.delete(index='meta', doc_type='meta', id=test_type)
    record = get_previous_index_record(True, True, es, test_type)
    assert record is None


def test_check_and_reindex_existing(app, testapp):
    from snovault.elasticsearch.create_mapping import check_and_reindex_existing
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    # post an item but don't reindex
    # this will cause the testing-ppp index to queue reindexing when we call
    # check_and_reindex_existing
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    # doc_count has not yet updated
    assert doc_count == 0
    test_uuids = set()
    check_and_reindex_existing(app, es, test_type, test_uuids)
    assert(len(test_uuids)) == 1
