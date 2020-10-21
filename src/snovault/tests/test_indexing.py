""" Test full indexing setup

The fixtures in this module setup a full system with postgresql and
elasticsearch running as subprocesses.
Does not include data dependent tests
"""

import pytest

pytestmark = [pytest.mark.indexing]


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture(scope='session')
def app_settings(wsgi_server_host_port, elasticsearch_server, postgresql_server, redis_server):
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
    settings['queue_type'] = 'Simple'
    settings['queue_server'] = True
    settings['queue_host'] = 'localhost'
    settings['queue_port'] = 6379
    settings['queue_worker'] = True
    settings['queue_worker_processes'] = 2
    settings['queue_worker_chunk_size'] = 1024
    settings['queue_worker_batch_size'] = 2000
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


def test_indexing_simple(testapp, indexer_testapp):
    # First post a single item so that subsequent indexing is incremental
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexed'] == 1
    assert 'txn_count' not in res.json

    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexed'] == 1
    assert res.json['title'] == 'primary_indexer'
    assert res.json['cycles'] > 0
    assert res.json.get('cycle_took') is not None
    assert res.json['txn_count'] == 1
    assert res.json['updated'] == [uuid]
    res = testapp.get('/search/?type=TestingPostPutPatch')
    assert res.json['total'] == 2


def test_indexer_state(dummy_request):
    from snovault.elasticsearch.indexer_state import IndexerState
    INDEX = dummy_request.registry.settings['snovault.elasticsearch.index']
    es = dummy_request.registry['elasticsearch']
    state = IndexerState(es,INDEX)
    result = state.get_initial_state()
    assert result['title'] == 'primary_indexer'
    result = state.start_cycle(['1','2','3'], result)
    assert result['cycle_count'] == 3
    assert result['status'] == 'indexing'
    cycles = result.get('cycles',0)
    result = state.finish_cycle(result, [])
    assert result['cycles'] == (cycles + 1)
    assert result['status'] == 'done'


def test_listening(testapp, listening_conn):
    import time
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(1)
    listening_conn.poll()
    assert len(listening_conn.notifies) == 1
    notify = listening_conn.notifies.pop()
    assert notify.channel == 'snovault.transaction'
    assert int(notify.payload) > 0
