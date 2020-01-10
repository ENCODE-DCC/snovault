import pytest


from snovault.util import _get_calculated_properties_from_paths
from snovault.util import select_distinct_values


# Test item with calculated property.
COLLECTION_URL = '/testing-link-targets/'


def test_get_calculated_properties_from_paths(dummy_request):
    paths = [
        COLLECTION_URL + 'ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert 'reverse' in calculated_properties


def test_not_a_collection_get_calculated_properties_from_paths(dummy_request):
    paths = [
        '/yxa/ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert not calculated_properties


def test_unformed_path_get_calculated_properties_from_paths(dummy_request):
    paths = [
        'testing-link-targets/ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert not calculated_properties


def test_select_distinct_values_returns_calculated(dummy_request, threadlocals, posted_targets_and_sources):
    distinct_values = select_distinct_values(dummy_request, 'reverse', *['/testing-link-targets/one/'])
    assert '/testing-link-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/' in distinct_values


def test_select_distinct_values_uses_calculated(dummy_request, threadlocals, posted_targets_and_sources, mocker):
    mocker.patch.object(dummy_request, 'embed')
    select_distinct_values(dummy_request, 'reverse', *['/testing-link-targets/one/'])
    dummy_request.embed.assert_called_with('/testing-link-targets/one/', '@@object')


def test_select_distinct_values_skips_calculated(dummy_request, threadlocals, posted_targets_and_sources, mocker):
    mocker.patch.object(dummy_request, 'embed')
    select_distinct_values(dummy_request, 'name', *['/testing-link-targets/one/'])
    dummy_request.embed.assert_called_with('/testing-link-targets/one/', '@@object?skip_calculated=true')


def test_types_utils_ensure_list():
    from snovault.util import ensure_list_and_filter_none
    assert ensure_list_and_filter_none('abc') == ['abc']
    assert ensure_list_and_filter_none(['abc']) == ['abc']
    assert ensure_list_and_filter_none({'a': 'b'}) == [{'a': 'b'}]
    assert ensure_list_and_filter_none([{'a': 'b'}, {'c': 'd'}]) == [{'a': 'b'}, {'c': 'd'}]
    assert ensure_list_and_filter_none([{'a': 'b'}, {'c': 'd'}, None]) == [{'a': 'b'}, {'c': 'd'}]


def test_types_utils_take_one_or_return_none():
    from snovault.util import take_one_or_return_none
    assert take_one_or_return_none(['just one']) == 'just one'
    assert take_one_or_return_none(['one', 'and', 'two']) is None
    assert take_one_or_return_none('just one') is None
