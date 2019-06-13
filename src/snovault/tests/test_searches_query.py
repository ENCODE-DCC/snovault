import pytest


@pytest.fixture
def query_dict():
    return {
        'query': {
            'bool': {
                'filter': [
                    {'term': {'embedded.@type': 'Experiment'}}
                ]
            }
        },
        'size': 0
    }


def test_searches_query_init():
    from snovault.searches.query import Query
    q = Query()
    assert isinstance(q, Query)


def test_searches_query_new_query():
    from snovault.searches.query import Query
    q = Query()
    nq = q.new_query()
    assert q is not nq


def test_searches_query_from_dict_to_dict(query_dict):
    from snovault.searches.query import Query
    q = Query.from_dict(query_dict)
    assert q.to_dict() == query_dict


def test_searches_query_using():
    from elasticsearch import Elasticsearch
    from snovault.searches.query import Query
    q = Query().using(Elasticsearch())
    assert isinstance(q.query._using, Elasticsearch)
