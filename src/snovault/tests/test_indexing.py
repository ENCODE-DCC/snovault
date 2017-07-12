""" Test full indexing setup

The fixtures in this module setup a full system with postgresql and
elasticsearch running as subprocesses.
Does not include data dependent tests
"""

import pytest
import time

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
    es = app.registry['elasticsearch']
    indexing_doc = es.get(index='meta', doc_type='meta', id='indexing')
    indexing_source = indexing_doc['_source']
    assert 'xmin' in indexing_source
    assert 'last_xmin' in indexing_source
    assert 'indexed' in indexing_source
    assert indexing_source['xmin'] >= indexing_source['last_xmin']
    testing_ppp_meta = es.get(index='meta', doc_type='meta', id='testing_post_put_patch')
    testing_ppp_source = testing_ppp_meta['_source']
    assert 'mapping' in testing_ppp_source
    assert 'settings' in testing_ppp_source


def test_listening(testapp, listening_conn):
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(1)
    listening_conn.poll()
    assert len(listening_conn.notifies) == 1
    notify = listening_conn.notifies.pop()
    assert notify.channel == 'snovault.transaction'
    assert int(notify.payload) > 0
