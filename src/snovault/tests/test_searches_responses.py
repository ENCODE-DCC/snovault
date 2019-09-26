import pytest


def test_searches_responses_fielded_response_init():
    from snovault.elasticsearch.searches.responses import FieldedResponse
    fr = FieldedResponse()
    assert isinstance(fr, FieldedResponse)


def test_searches_responses_fielded_response_validate_response_fields():
    from snovault.elasticsearch.searches.responses import FieldedResponse
    from snovault.elasticsearch.searches.fields import ResponseField
    rf = ResponseField()
    FieldedResponse(response_fields=[rf])
    class NewResponseField(ResponseField):
        def __init__(self):
            super().__init__()
    nrf = NewResponseField()
    FieldedResponse(response_fields=[rf, nrf])
    class OtherResponseField():
        pass
    orf = OtherResponseField()
    with pytest.raises(ValueError):
        FieldedResponse(response_fields=[rf, nrf, orf])


def test_searches_responses_query_response_init():
    from snovault.elasticsearch.searches.responses import QueryResponse
    qr = QueryResponse([], [])
    assert isinstance(qr, QueryResponse)


def test_searches_responses_basic_query_response_with_facets_init():
    from snovault.elasticsearch.searches.responses import BasicQueryResponseWithFacets
    bqr = BasicQueryResponseWithFacets([], [])
    assert isinstance(bqr, BasicQueryResponseWithFacets)


def test_searches_responses_raw_query_response_with_aggs_init():
    from snovault.elasticsearch.searches.responses import RawQueryResponseWithAggs
    rqr = RawQueryResponseWithAggs([], [])
    assert isinstance(rqr, RawQueryResponseWithAggs)


def test_searches_responses_basic_matrix_response_with_facets_init():
    from snovault.elasticsearch.searches.responses import BasicMatrixResponseWithFacets
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    from snovault.elasticsearch.searches.mixins import AggsToMatrixMixin
    bmr = BasicMatrixResponseWithFacets([], [])
    assert isinstance(bmr, BasicMatrixResponseWithFacets)
    assert isinstance(bmr, AggsToFacetsMixin)
    assert isinstance(bmr, AggsToMatrixMixin)


def test_searches_responses_audit_matrix_response_with_facets_init():
    from snovault.elasticsearch.searches.responses import AuditMatrixResponseWithFacets
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    from snovault.elasticsearch.searches.mixins import AuditAggsToMatrixMixin
    amr = AuditMatrixResponseWithFacets([], [])
    assert isinstance(amr, AuditMatrixResponseWithFacets)
    assert isinstance(amr, AggsToFacetsMixin)
    assert isinstance(amr, AuditAggsToMatrixMixin)
