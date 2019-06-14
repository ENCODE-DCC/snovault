import pytest


def test_searches_builders_response_builder_init():
    from snovault.elasticsearch.searches.builders import ResponseBuilder
    rb = ResponseBuilder()
    assert isinstance(rb, ResponseBuilder)


def test_searches_builders_query_builder_init():
    from snovault.elasticsearch.searches.builders import QueryBuilder
    qb = QueryBuilder({})
    assert isinstance(qb, QueryBuilder)


def test_searches_builders_basic_search_query_builder_init():
    from snovault.elasticsearch.searches.builders import BasicSearchQuery
    bsq = BasicSearchQuery({})
    assert isinstance(bsq, BasicSearchQuery)
