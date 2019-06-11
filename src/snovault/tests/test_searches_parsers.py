import pytest
from pyramid.security import effective_principals


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


def test_searches_params_parser_query_string_not(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type!=Experiment'
    p = ParamsParser(dummy_request)
    assert 'type!' in p._request.params


def test_searches_params_parser_get_filters_by_condition_none(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition() == [
        ('type', 'Experiment'),
        ('type', 'File'),
        ('field', 'status')
    ]


def test_searches_params_parser_get_filters_by_condition_key_field(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'field'
    ) == [
        ('field', 'status')
    ]


def test_searches_params_parser_get_filters_by_condition_key_type(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'type'
    ) == [
        ('type', 'Experiment'),
        ('type', 'File')
    ]


def test_searches_params_parser_get_filters_by_condition_value_status(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        value_condition=lambda v: v == 'status'
    ) == [
        ('field', 'status')
    ]


def test_searches_params_parser_get_filters_by_condition_key_type_value_file(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'type',
        value_condition=lambda v: v == 'File'
    ) == [
        ('type', 'File')
    ]


def test_searches_params_parser_get_filters_by_condition_contains_letter(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'type',
        value_condition=lambda v: v == 'File'
    ) == [
        ('type', 'File')
    ]
