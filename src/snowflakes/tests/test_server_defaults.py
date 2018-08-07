from pytest import fixture


COLLECTION_URL = '/testing-server-defaults'


@fixture
def extra_environ(admin):
    email = admin['email']
    return {'REMOTE_USER': str(email)}


@fixture
def default_content(testapp, external_tx):
    res = testapp.post_json(COLLECTION_URL, {}, status=201)
    return {'@id': res.json['@graph'][0]['@id']}


def test_server_defaults(admin, anontestapp, extra_environ):
    res = anontestapp.post_json(
        COLLECTION_URL, {}, status=201,
        extra_environ=extra_environ,
    )
    item = res.json['@graph'][0]
    assert item['now'].startswith('2')
    assert item['user'] == admin['@id']
    assert item['accession'].startswith('SNO')

    anontestapp.patch_json(
        res.location, {}, status=200,
        extra_environ=extra_environ,
    )


@fixture(scope='session')
def test_accession_app(request, check_constraints, zsa_savepoints, app_settings):
    from snowflakes import main
    app_settings = app_settings.copy()
    app_settings['accession_factory'] = 'snowflakes.server_defaults.test_accession'
    return main({}, **app_settings)


@fixture
def test_accession_anontestapp(request, test_accession_app, external_tx, zsa_savepoints):
    '''TestApp with JSON accept header.
    '''
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
    }
    return TestApp(test_accession_app, environ)


@fixture
def test_accession_authtestapp(request, test_accession_app, external_tx, zsa_savepoints):
    '''TestApp with JSON accept header.
    '''
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': 'TEST_AUTHENTICATED',
    }
    return TestApp(test_accession_app, environ)


def test_test_accession_server_defaults(admin, test_accession_anontestapp, extra_environ):
    res = test_accession_anontestapp.post_json(
        COLLECTION_URL, {}, status=201,
        extra_environ=extra_environ,
    )
    item = res.json['@graph'][0]
    assert item['accession'].startswith('TSTSS')

    test_accession_anontestapp.patch_json(
        res.location, {}, status=200,
        extra_environ=extra_environ,
    )
