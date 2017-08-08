import pytest

def test_get_jsonld_types_from_collection_type(app):
    from snovault.fourfront_utils import get_jsonld_types_from_collection_type
    test_item_type = 'Snowset'
    expected_types = ['snowball', 'snowfort']
    res = get_jsonld_types_from_collection_type(app, test_item_type)
    assert sorted(res) == expected_types
