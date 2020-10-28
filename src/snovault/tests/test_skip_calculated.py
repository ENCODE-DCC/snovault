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


def test_filtered_object_view_get_default(testapp, dummy_request, posted_targets_and_sources):
    r = testapp.get('/testing-link-targets/one/')
    expected_keys = [
        'name',
        '@id',
        '@type',
        'uuid',
        'reverse',
        '@context'
    ]
    assert all(
        [
            e in r.json
            for e in expected_keys
        ]
    )


def test_filtered_object_view_get_filtered_object(testapp, dummy_request, posted_targets_and_sources):
    r = testapp.get('/testing-link-targets/one/@@filtered_object')
    expected_keys = [
        'name',
        '@id',
        '@type',
        'uuid',
        'reverse',
    ]
    assert all(
        [
            e in r.json
            for e in expected_keys
        ]
    )


def test_filtered_object_view_get_filtered_object_with_exclude(testapp, dummy_request, posted_targets_and_sources):
    r = testapp.get('/testing-link-targets/one/@@filtered_object?exclude=reverse')
    expected_keys = [
        'name',
        '@id',
        '@type',
        'uuid',
    ]
    assert all(
        [
            e in r.json
            for e in expected_keys
        ]
    )
    assert 'reverse' not in r.json


def test_filtered_object_view_get_filtered_object_with_more_exclude(testapp, dummy_request, posted_targets_and_sources):
    r = testapp.get('/testing-link-targets/one/@@filtered_object?exclude=reverse&exclude=@id')
    expected_keys = [
        'name',
        '@type',
        'uuid',
    ]
    assert all(
        [
            e in r.json
            for e in expected_keys
        ]
    )
    assert 'reverse' not in r.json
    assert '@id' not in r.json


def test_filtered_object_view_get_filtered_object_with_exclude_and_include(testapp, dummy_request, posted_targets_and_sources):
    r = testapp.get('/testing-link-targets/one/@@filtered_object?exclude=reverse&exclude=@id&include=uuid')
    expected_keys = [
        'uuid',
    ]
    assert all(
        [
            e in r.json
            for e in expected_keys
        ]
    )
    assert 'reverse' not in r.json
    assert '@id' not in r.json
    assert 'name' not in r.json
    assert '@type' not in r.json


def test_embedded_with_frame_custom_embed(testapp, dummy_request, posted_custom_embed_targets_and_sources):
    r = testapp.get('/testing-custom-embed-targets/one/')
    assert r.json['reverse'][0] == {
        'name': 'A',
        'status': 'current',
        'target': '/testing-custom-embed-targets/one/',
        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/',
        '@type': ['TestingCustomEmbedSource', 'Item'],
        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd'
    }
    assert r.json['filtered_reverse'][0] == {
        'status': 'current',
        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd'
    }
    assert r.json['filtered_reverse1'][0] == {
        'name': 'A',
        'status': 'current',
        'target': '/testing-custom-embed-targets/one/',
        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
    }
    assert r.json['reverse_uncalculated'][0] == {
        'name': 'A',
        'status': 'current',
        'target': '/testing-custom-embed-targets/one/'
    }
