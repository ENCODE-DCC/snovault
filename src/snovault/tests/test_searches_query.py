import pytest


def test_searches_builders_query_init():
    from snovault.searches.query import Query
    q = Query()
    assert isinstance(qb, Query)
