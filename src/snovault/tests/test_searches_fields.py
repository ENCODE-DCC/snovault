import pytest


@pytest.fixture()
def dummy_parent(dummy_request):
    from pyramid.testing import DummyResource
    from pyramid.security import Allow
    from snosearch.parsers import ParamsParser
    from snosearch.queries import AbstractQueryFactory
    from snovault.elasticsearch.interfaces import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    dummy_request.context = DummyResource()
    dummy_request.context.__acl__ = lambda: [(Allow, 'group.submitter', 'search_audit')]
    class DummyParent():
        def __init__(self):
            self._meta = {}
            self.response = {}
    dp = DummyParent()
    pp = ParamsParser(dummy_request)
    dp._meta = {
        'params_parser': pp,
        'query_builder': AbstractQueryFactory(pp)
    }
    return dp


def test_searches_fields_non_sortable_response_field(dummy_parent):
    dummy_parent._meta['params_parser']._request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema'
    )
    from snovault.elasticsearch.searches.fields import NonSortableResponseField
    nrf = NonSortableResponseField()
    r = nrf.render(parent=dummy_parent)
    assert r['non_sortable'] == ['pipeline_error_detail', 'description', 'notes']
