from pytest import fixture


COLLECTION_URL = '/testing-server-defaults'

@fixture
def extra_environ(admin):
    email = admin['email']
    return {'REMOTE_USER': str(email)}


@fixture
def default_content(testapp, external_tx):
    res = testapp.post_json(COLLECTION_URL, {}, status=201)
    return {'@id': res.location}


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
    assert item['accession'].startswith('TSTAB')

    test_accession_anontestapp.patch_json(
        res.location, {}, status=200,
        extra_environ=extra_environ,
    )


def test_batch_upgrade_error(default_content, root, test_accession_authtestapp, monkeypatch):
    collection = root['testing_server_default']
    properties = collection.type_info.schema['properties']

    url = default_content['@id']
    res = test_accession_authtestapp.get(url)
    assert res.json['schema_version'] == '1'

    monkeypatch.setitem(properties['schema_version'], 'default', '2')
    monkeypatch.setattr(collection.type_info, 'schema_version', '2')
    monkeypatch.setitem(properties, 'new_property', {'default': 'new'})
    res = test_accession_authtestapp.patch_json(url, {}, status=200)
    assert res.json['@graph'][0]['schema_version'] == '2'
    assert res.json['@graph'][0]['new_property'] == 'new'
