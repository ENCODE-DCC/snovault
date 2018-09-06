import pytest


def test_object_view_get_default(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/')
    assert 'reverse' in res.json


def test_object_view_get_skip_calcualted(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/?frame=object&skip_calculated=true')
    assert 'reverse' not in res.json


def test_object_view_embed_default(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed('/testing-link-targets/one/', '@@object')
    assert 'reverse' in res


def test_object_view_embed_skip_calculated(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed(
        '/testing-link-targets/one/',
        '@@object?skip_calculated=true'
    )
    assert 'reverse' not in res
