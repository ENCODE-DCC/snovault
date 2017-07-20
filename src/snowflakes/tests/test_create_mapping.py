import pytest
from ..loadxl import ORDER
from snovault.elasticsearch.interfaces import ELASTIC_SEARCH

@pytest.mark.parametrize('item_type', ORDER)
def test_type_mapping(registry, item_type):
    """
    Test basic mapping properties for each item type
    """
    from snovault.elasticsearch.create_mapping import type_mapping
    from snovault import TYPES
    mapping = type_mapping(registry[TYPES], item_type)
    assert mapping
    assert 'properties' in mapping
    assert 'include_in_all' in mapping
