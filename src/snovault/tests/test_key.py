import pytest

# Test for storage.keys

pytest_plugins = [
    'snovault.tests.serverfixtures',
    'snovault.tests.testappfixtures',
]

items = [
    {'name': 'one', 'alias': 'TEST1'},
    {'name': 'two', 'alias': 'TEST2'},
]

bad_items = [
    {'name': 'one', 'alias': 'BAD1'},
    {'name': 'bad', 'alias': 'TEST1'},
]


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture(scope='session')
def app(DBSession, app_settings):
    from pyramid.config import Configurator
    from snovault import DBSESSION
    config = Configurator()
    config.registry.settings['local_storage_host'] = app_settings['local_storage_host']
    config.registry.settings['local_storage_port'] = app_settings['local_storage_port']
    config.registry.settings['local_storage_redis_index'] = app_settings['local_storage_redis_index']
    config.registry.settings['local_storage_timeout'] = app_settings['local_storage_timeout']
    config.registry.settings['local_tz'] = app_settings['local_tz']
    config.registry[DBSESSION] = DBSession
    config.include('snovault')
    config.include('.testing_key')
    return config.make_wsgi_app()


@pytest.fixture
def content(testapp):
    url = '/testing-keys/'
    for item in items:
        testapp.post_json(url, item, status=201)


@pytest.mark.parametrize('item', items)
def test_unique_key(testapp, content, item):
    url = '/testing-keys/' + item['alias']
    res = testapp.get(url).maybe_follow()
    assert res.json['name'] == item['name']


def test_keys_bad_items(testapp, content):
    url = '/testing-keys/'
    for item in bad_items:
        testapp.post_json(url, item, status=409)


def test_keys_update(testapp):
    url = '/testing-keys/'
    item = items[0]
    res = testapp.post_json(url, item, status=201)
    location = res.location
    new_item = {'name': 'new_one', 'alias': 'NEW1'}
    testapp.put_json(location, new_item, status=200)
    testapp.post_json(url, item, status=201)
    testapp.put_json(location, item, status=409)


def test_keys_conflict(testapp):
    url = '/testing-keys/'
    item = items[1]
    initial = testapp.get(url).json['@graph']
    testapp.post_json(url, item, status=201)
    posted = testapp.get(url).json['@graph']
    assert(len(initial)+1 == len(posted))
    conflict = testapp.post_json(url, item, status=409)
    assert(conflict.status_code == 409)
    conflicted = testapp.get(url).json['@graph']
    assert(len(posted) == len(conflicted))
