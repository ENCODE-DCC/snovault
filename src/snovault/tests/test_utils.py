import pytest

from snovault.util import (
    select_distinct_values,
    _get_calculated_properties_from_paths,
)

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
