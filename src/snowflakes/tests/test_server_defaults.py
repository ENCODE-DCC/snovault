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
    assert item['accession'].startswith('TSTAB')

    test_accession_anontestapp.patch_json(
        res.location, {}, status=200,
        extra_environ=extra_environ,
    )


def test_batch_upgrade_error(admin, root, test_accession_anontestapp, extra_environ, monkeypatch):
    res = test_accession_anontestapp.post_json(
        '/testing-bad-accession/', {}, status=201,
        extra_environ=extra_environ,
    )
    collection = root['testing_bad_accession']
    properties = collection.type_info.schema['properties']

    test_ob = res.json['@graph'][0]
    url = test_ob['@id']
    res = test_accession_anontestapp.get(url, extra_environ=extra_environ)
    assert res.json['schema_version'] == '2'

    schema_update = {
        'type': 'string',
    }

    assert collection.type_info.schema_version == '2'
    # downgrade via monkey patch
    monkeypatch.setitem(properties['schema_version'], 'default', '1')
    monkeypatch.setitem(properties, 'accession', schema_update)
    monkeypatch.setattr(collection.type_info, 'schema_version', '1')
    monkeypatch.setitem(collection.type_info.schema['properties'], 'accession', schema_update)
    patched_res = test_accession_anontestapp.patch_json(
        url, {'accession': 'totallyWrong', 'schema_version': "1"}, status=200,
        extra_environ=extra_environ)
    assert patched_res.json['@graph'][0]['schema_version'] == '1'

    upgrade = test_accession_anontestapp.post_json(
        '/batch_upgrade', {'batch': [test_ob['uuid']]}, status=200,
        extra_environ=extra_environ)

    assert upgrade.json['results'][0] == [
        'testing_bad_accession',
        test_ob['uuid'],
        False,
        True,
    ]
