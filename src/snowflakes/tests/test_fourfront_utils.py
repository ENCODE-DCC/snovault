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
    processed_embeds = {'obj1.*'}
    final_embeds = build_default_embeds(embeds_to_add, processed_embeds)
    expected_embeds = [
        'obj1.obj2.link_id',
        'obj1.obj2.display_title',
        'obj1.obj2.@id',
        'obj1.obj2.uuid',
        'obj1.obj2.principals_allowed.*',
        'obj1.*',
        'obj1.obj3.*'
    ]
    assert(set(final_embeds) == set(expected_embeds))


def test_find_default_embeds_and_expand_emb_list(registry):
    from snovault.fourfront_utils import find_default_embeds_for_schema, expand_embedded_list
    from snovault import TYPES
    # use snowflake as test case
    type_info = registry[TYPES].by_item_type['snowflake']
    schema_props = type_info.schema.get('properties')
    default_embeds = find_default_embeds_for_schema('', schema_props)
    expected_embeds = ['lab', 'award', 'submitted_by', 'snowset', 'principals_allowed.*']
    assert(set(default_embeds) == set(expected_embeds))
    # lets use the default embeds as an "embedded_list" for snowflake
    dummy_emb_list = [emb + '.*' if not emb.endswith('*') else emb for emb in expected_embeds ]
    embs_to_add, proc_embs = expand_embedded_list('snowflake', registry[TYPES], dummy_emb_list, schema_props, set())
    expected_to_add = ['lab.pi', 'lab.awards', 'lab.principals_allowed.*', 'award.pi', 'award.principals_allowed.*', 'submitted_by.lab', 'submitted_by.submits_for', 'submitted_by.principals_allowed.*', 'snowset.lab', 'snowset.award', 'snowset.principals_allowed.*', 'snowset.submitted_by']
    assert(set(embs_to_add) == set(expected_to_add))
