import pytest
from snowflakes.types.snow import Snowflake
from snovault import TYPES

# test for default_embedding practice with embedded list
# this test should change should any of the reference embeds below be altered
def test_snowflake_embedded_list(registry):
    sno_data = {
        'status': 'in review by lab'
    }
    test_sno = Snowflake.create(registry, None, sno_data)
    # call reify embedded property (defined in snovault/resources.py)
    embedded = test_sno.embedded
    embedded_list = test_sno.embedded_list
    type_info_embedded = registry[TYPES]['snowflake'].embedded_list
    assert type_info_embedded == embedded_list
    # since embedded is an expanded embedded_list
    # everthing in embedded_list will be in embedded
    for embed in embedded_list:
        assert embed in embedded
    if 'lab.awards.project' in embedded_list:
        assert 'lab.awards.uuid' in embedded
        assert 'lab.awards.display_title' in embedded
        assert 'lab.awards.link_id' in embedded
