import pytest


def test_searches_fields_abstract_response_field_init():
    from snovault.elasticsearch.searches.fields import AbstractResponseField
    arf = AbstractResponseField()
    assert isinstance(arf, AbstractResponseField)


def test_searches_fields_basic_search_response_field_init():
    from snovault.elasticsearch.searches.fields import BasicSearchResponseField
    brf = BasicSearchResponseField()
    assert isinstance(brf, BasicSearchResponseField)
