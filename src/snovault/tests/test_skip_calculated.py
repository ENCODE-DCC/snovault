import pytest


items_with_calculated_property = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
]


@pytest.fixture
def posted_targets(testapp):
    url = '/testing-link-targets/'
    for item in items_with_calculated_property:
        testapp.post_json(url, item, status=201)


def test_object_view_get_default(testapp, dummy_request, posted_targets):
    res = testapp.get('/testing-link-targets/one/')
    assert 'reverse' in res.json


def test_object_view_get_skip_calcualted(testapp, dummy_request, posted_targets):
    res = testapp.get('/testing-link-targets/one/?frame=object&skip_calculated=true')
    assert 'reverse' not in res.json


def test_object_view_embed_default(dummy_request, threadlocals, posted_targets):
    res = dummy_request.embed('/testing-link-targets/one/', '@@object')
    assert 'reverse' in res


def test_object_view_embed_skip_calculated(dummy_request, threadlocals, posted_targets):
    res = dummy_request.embed(
        '/testing-link-targets/one/',
        '@@object?skip_calculated=true'
    )
    assert 'reverse' not in res
