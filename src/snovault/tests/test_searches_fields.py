import pytest


def test_searches_fields_response_field_init():
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    assert isinstance(rf, ResponseField)


def test_searches_fields_basic_search_response_field_init():
    from snovault.elasticsearch.searches.fields import BasicSearchResponseField
    brf = BasicSearchResponseField()
    assert isinstance(brf, BasicSearchResponseField)
