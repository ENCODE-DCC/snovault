import pytest


def test_searches_params_parser_init(dummy_request):
    from snovault.searches.parsers import ParamsParser
    p = ParamsParser(dummy_request)
    assert isinstance(p, ParamsParser)
    assert p._request is dummy_request
