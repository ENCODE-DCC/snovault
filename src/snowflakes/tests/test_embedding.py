import pytest
from ..loadxl import ORDER

@pytest.mark.parametrize('item_type', ORDER)
def test_add_default_embeds(registry, item_type):
    """
    Ensure default embedding matches the schema for each object
    """
    from snovault.fourfront_utils import add_default_embeds, crawl_schemas_by_embeds
    from snovault import TYPES
    type_info = registry[TYPES].by_item_type[item_type]
    schema = type_info.schema
    embeds = add_default_embeds(item_type, registry[TYPES], type_info.embedded, schema)
    for embed in embeds:
        split_embed = embed.strip().split('.')
        error, added_embeds = crawl_schemas_by_embeds(item_type, registry[TYPES], split_embed, schema['properties'])
        assert error is None


@pytest.mark.parametrize('item_type', ORDER)
def test_manual_embeds(registry, item_type):
    """
    Ensure manual embedding in the types files are valid
    """
    from snovault.fourfront_utils import crawl_schemas_by_embeds
    from snovault import TYPES
    type_info = registry[TYPES].by_item_type[item_type]
    schema = type_info.schema
    embeds = type_info.embedded
    for embed in embeds:
        split_embed = embed.strip().split('.')
        error, added_embeds = crawl_schemas_by_embeds(item_type, registry[TYPES], split_embed, schema['properties'])
        assert error is None
