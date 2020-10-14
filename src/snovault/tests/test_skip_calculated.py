import pytest


def test_object_view_get_default(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/')
    assert 'reverse' in res.json


def test_object_view_get_skip_calcualted(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/?frame=object&skip_calculated=true')
    assert 'reverse' not in res.json


def test_object_view_get_object_with_select_calculated_properties_none(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/?frame=object_with_select_calculated_properties')
    assert 'reverse' not in res.json


def test_object_view_get_object_with_select_calculated_properties_reverse(testapp, dummy_request, posted_targets_and_sources):
    res = testapp.get('/testing-link-targets/one/?frame=object_with_select_calculated_properties&field=reverse')
    assert 'reverse' in res.json


def test_object_view_embed_default(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed('/testing-link-targets/one/', '@@object')
    assert 'reverse' in res


def test_object_view_embed_skip_calculated(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed(
        '/testing-link-targets/one/',
        '@@object?skip_calculated=true'
    )
    assert 'reverse' not in res


def test_object_view_embed_object_with_select_calculated_properties_none(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed('/testing-link-targets/one/', '@@object_with_select_calculated_properties')
    assert 'reverse' not in res


def test_object_view_embed_object_with_select_calculated_properties_reverse(dummy_request, threadlocals, posted_targets_and_sources):
    res = dummy_request.embed(
        '/testing-link-targets/one/',
        '@@object_with_select_calculated_properties?field=reverse&field=nonsense'
    )
    assert 'reverse' in res


def test_calculated_should_render_property():
    from snovault.calculated import _should_render_property
    assert _should_render_property(None, None, 'abc')
    assert _should_render_property(None, [], 'abc')
    assert not _should_render_property([], None, 'abc')
    assert not _should_render_property([], [], 'abc')
    assert _should_render_property(['abc'], None, 'abc')
    assert _should_render_property(['abc', 'xyz'], None, 'abc')
    assert not _should_render_property(['xyz'], None, 'abc')
    assert not _should_render_property(['xyz'], None, 'abc')
    assert not _should_render_property(['xyz'], [], 'abc')
    assert not _should_render_property(['xyz'], ['abc'], 'abc')
    assert not _should_render_property(['xyz'], ['abc'], 'abc')
    assert _should_render_property(['abc', 'xyz'], [], 'abc')
    assert _should_render_property(['xyz'], [], 'xyz')
