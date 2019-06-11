import pytest


def test_searches_params_parser_init(dummy_request):
    from snovault.searches.parsers import ParamsParser
    p = ParamsParser(dummy_request)
    assert isinstance(p, ParamsParser)
    assert p._request is dummy_request


def test_searches_params_parser_query_string(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment'
    p = ParamsParser(dummy_request)
    assert 'type' in p._request.params
    assert p._request.params.getall('type') == ['Experiment']
