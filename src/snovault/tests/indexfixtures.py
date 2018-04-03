import pytest


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

    from snovault import DBSESSION
    DBSession = app.registry[DBSESSION]
    # Dispose connections so postgres can tear down.
    DBSession.bind.pool.dispose()


@pytest.fixture(scope='session')
def DBSession(app):
    from snovault import DBSESSION
    return app.registry[DBSESSION]


@pytest.fixture(autouse=True)
def teardown(app):
    from snovault.elasticsearch import create_mapping
    import transaction
    create_mapping.run(app, skip_indexing=True)
    # connection = DBEngine.connect()  # use for .execute
    from sqlalchemy import MetaData
    # connection = DBSession.connection().connect()
    # DBMetaData = MetaData(bind=DBSession.connection(), reflect=True)
    # or
    import pdb; pdb.set_trace()
    from snovault import DBSESSION
    session = app.registry[DBSESSION]
    DBMetaData = MetaData(bind=session.connection(), reflect=True)
    connection = session.connection().connect()
    for table in reversed(DBMetaData.sorted_tables):
        print('Clear table %s' % table)
        # DBConnection.execute(table.delete())
        connection.execute(table.delete())
    transaction.commit()


@pytest.fixture(scope='session')
def DBSession(app):
    from snovault import DBSESSION
    return app.registry[DBSESSION]


# @pytest.fixture(scope='session')
# def DBEngine(DBSession):
#     return DBSession.connection()

@pytest.yield_fixture
def DBConnection(DBSession):
    yield DBSession.connection().connect()


@pytest.yield_fixture
def DBMetaData(DBSession):
    from sqlalchemy import MetaData
    yield MetaData(bind=DBSession.connection(), reflect=True)

#
#
# @pytest.fixture(autouse=True)
# def teardown(app, DBSession):
#     from snovault.elasticsearch import create_mapping
#     from snovault.elasticsearch.interfaces import INDEXER_QUEUE
#     import pdb; pdb.set_trace()
#     create_mapping.run(app, skip_indexing=True)
#     app.registry[INDEXER_QUEUE].clear_queue()
#     # ... maybe ...
#     # DBSession.close()
#     # DBSession.rollback()
#     DBSession.execute("""TRUNCATE resources, transactions CASCADE;""")
