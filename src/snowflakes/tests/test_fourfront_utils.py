import pytest

def test_get_jsonld_types_from_collection_type(app):
    from snovault.fourfront_utils import get_jsonld_types_from_collection_type
    test_item_type = 'Snowset'
    expected_types = ['snowball', 'snowfort']
    res = get_jsonld_types_from_collection_type(app, test_item_type)
    assert sorted(res) == expected_types


def test_build_default_embeds():
    from snovault.fourfront_utils import build_default_embeds
    """ simple unit test """
    embeds_to_add = ['obj1.obj2', 'obj1', 'obj1.obj3.*']
    processed_embeds = ['obj1.*']
    final_embs = build_default_embeds(embeds_to_add, processed_embeds)
    expected_embs = [
        'obj1.obj2.link_id',
        'obj1.obj2.display_title',
        'obj1.obj2.@id',
        'obj1.obj2.uuid',
        'obj1.obj2.principals_allowed.*',
        'obj1.*',
        'obj1.obj3.*'
    ]
    assert(set(final_embs) == set(expected_embs))

def test_find_default_embeds_for_schema(app):
    from snovault.fourfront_utils import find_default_embeds_for_schema
    from snovault import TYPES
    import pdb; pdb.set_trace()
    type_info = registry[TYPES].by_item_type['snowset']
    schema_props = type_info.schema.get('properties')
    default_embeds = find_default_embeds_for_schema('', schema_props)
    assert(True)
