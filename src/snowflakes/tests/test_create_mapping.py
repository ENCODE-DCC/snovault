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
    import pdb; pdb.set_trace()
    assert mapping
    assert 'properties' in mapping
    assert 'include_in_all' in mapping


def test_create_mapping(app, testapp, registry):
    """
    Test overall create_mapping functionality using app.
    Do this by checking es directly before and after running mapping.
    Delete an index directly, run again to see if it recovers.
    """
    from snovault.elasticsearch import create_mapping
    assert True


@pytest.mark.parametrize('item_type', ORDER)
def test_indexed_data(registry, item_type):
    """
    Get es results directly and test to make sure the _embedded results
    match with the embedded list in the types files.
    """
    from snovault import TYPES
    type_info = registry[TYPES].by_item_type[item_type]
    embedded = type_info.embedded
    assert True
