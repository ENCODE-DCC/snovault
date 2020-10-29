import pytest

_app_settings = {
    'collection_datastore': 'database',
    'item_datastore': 'database',
    'load_test_only': True,
    'testing': True,
    'pyramid.debug_authorization': True,
    'postgresql.statement_timeout': 20,
    'retry.attempts': 3,
    'multiauth.policies': 'session remoteuser accesskey webuser',
    'multiauth.groupfinder': 'snowflakes.authorization.groupfinder',
    'multiauth.policy.session.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.session.base': 'pyramid.authentication.SessionAuthenticationPolicy',
    'multiauth.policy.session.namespace': 'mailto',
    'multiauth.policy.remoteuser.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.remoteuser.namespace': 'remoteuser',
    'multiauth.policy.remoteuser.base': 'pyramid.authentication.RemoteUserAuthenticationPolicy',
    'multiauth.policy.accesskey.use': 'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.accesskey.namespace': 'accesskey',
    'multiauth.policy.accesskey.base': 'snovault.authentication.BasicAuthAuthenticationPolicy',
    'multiauth.policy.accesskey.check': 'snovault.authentication.basic_auth_check',
    'multiauth.policy.webuser.use':  'snovault.authentication.NamespacedAuthenticationPolicy',
    'multiauth.policy.webuser.namespace': 'webuser',
    'multiauth.policy.webuser.base': 'snovault.authentication.WebUserAuthenticationPolicy',
    # Local Storage
    'local_storage_host': 'localhost',
    'local_storage_port': 6378,
    'local_storage_redis_index': 0,
    'local_storage_timeout': 5,
    'local_tz': 'GMT',
}


@pytest.fixture(scope='session')
def app_settings(request, wsgi_server_host_port, conn, DBSession, redis_server):
    from snovault import DBSESSION
    settings = _app_settings.copy()
    settings[DBSESSION] = DBSession
    return settings


@pytest.fixture(scope='session')
def app(app_settings):
    '''WSGI application level functional testing.
       will have to make snovault dummy main app
    '''
    from snovault import main
    return main({}, **app_settings)


@pytest.fixture
def testapp(app):
    '''TestApp with JSON accept header.
    '''
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST',
    }
    return TestApp(app, environ)


@pytest.fixture
def anontestapp(app):
    '''TestApp with JSON accept header.
    '''
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
    }
    return TestApp(app, environ)


@pytest.fixture
def authenticated_testapp(app):
    '''TestApp with JSON accept header for non-admin user.
    '''
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST_AUTHENTICATED',
    }
    return TestApp(app, environ)


@pytest.fixture
def embed_testapp(app):
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'EMBED',
    }
    return TestApp(app, environ)


@pytest.fixture
def indexer_testapp(app):
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'INDEXER',
    }
    return TestApp(app, environ)
