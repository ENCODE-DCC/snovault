import pytest
import transaction
from sqlalchemy import MetaData
from snovault import DBSESSION

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

    DBSession = app.registry[DBSESSION]
    # Dispose connections so postgres can tear down.
    DBSession.bind.pool.dispose()


@pytest.fixture(autouse=True)
def teardown(app):
    from snovault.elasticsearch import create_mapping
    create_mapping.run(app, skip_indexing=True)
    session = app.registry[DBSESSION]
    connection = session.connection().connect()
    meta = MetaData(bind=session.connection(), reflect=True)
    # print('BEFORE -->', str(connection.cursor().scalar("SELECT COUNT(*) FROM current_propsheets")))
    # meta.drop_all()
    # meta.create_all()
    # print('AFTER -->', str(connection.cursor().scalar("SELECT COUNT(*) FROM current_propsheets")), '\n')
    for table in meta.sorted_tables:
        print('BEFORE -->', str(connection.scalar("SELECT COUNT(*) FROM current_propsheets")))
        print('Clear table %s' % table)
        connection.execute(table.delete())
        print('AFTER -->', str(connection.scalar("SELECT COUNT(*) FROM current_propsheets")), '\n')
    transaction.commit()
