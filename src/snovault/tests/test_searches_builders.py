import pytest


def test_searches_builders_response_builder_init():
    from snovault.elasticsearch.searches.builders import ResponseBuilder
    rb = ResponseBuilder()
    assert isinstance(rb, ResponseBuilder)
