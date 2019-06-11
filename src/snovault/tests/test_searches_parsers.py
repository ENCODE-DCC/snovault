import pytest
from pyramid.security import effective_principals


def test_searches_parsers_params_parser_init(dummy_request):
    from snovault.searches.parsers import ParamsParser
    p = ParamsParser(dummy_request)
    assert isinstance(p, ParamsParser)
    assert p._request is dummy_request


def test_searches_parsers_params_parser_query_string(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment'
    p = ParamsParser(dummy_request)
    assert 'type' in p._request.params
    assert p._request.params.getall('type') == ['Experiment']


def test_searches_parsers_params_parser_query_string_not(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type!=Experiment'
    p = ParamsParser(dummy_request)
    assert 'type!' in p._request.params


def test_searches_parsers_params_parser_get_filters_by_condition_none(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition() == [
        ('type', 'Experiment'),
        ('type', 'File'),
        ('field', 'status')
    ]


def test_searches_parsers_params_parser_get_filters_by_condition_key_field(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'field'
    ) == [
        ('field', 'status')
    ]


def test_searches_parsers_params_parser_get_filters_by_condition_key_type(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'type'
    ) == [
        ('type', 'Experiment'),
        ('type', 'File')
    ]


def test_searches_parsers_params_parser_get_filters_by_condition_value_status(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        value_condition=lambda v: v == 'status'
    ) == [
        ('field', 'status')
    ]


def test_searches_parsers_params_parser_get_filters_by_condition_key_type_value_file(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: k == 'type',
        value_condition=lambda v: v == 'File'
    ) == [
        ('type', 'File')
    ]


def test_searches_parsers_params_parser_get_filters_by_condition_contains_letter(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = 'type=Experiment&type=File&field=status'
    p = ParamsParser(dummy_request)
    assert p.get_filters_by_condition(
        key_condition=lambda k: 't' in k,
        value_condition=lambda v: 'F' in v
    ) == [
        ('type', 'File')
    ]


def test_searches_parsers_params_parser_get_key_filters(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&status=archived&type!=Item&status!=released'
    )
    p = ParamsParser(dummy_request)
    assert p.get_key_filters(key='status') == [
        ('status', 'archived'),
        ('status!', 'released')
    ]


def test_searches_parsers_params_parser_get_type_filters(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&field=status&type!=Item'
    )
    p = ParamsParser(dummy_request)
    assert p.get_type_filters() == [
        ('type', 'Experiment'),
        ('type', 'File'),
        ('type!', 'Item')
    ]


def test_searches_parsers_params_parser_get_search_term_filters_empty(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&field=status&type!=Item'
    )
    p = ParamsParser(dummy_request)
    assert p.get_search_term_filters() == []


def test_searches_parsers_params_parser_get_search_term_filters(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&field=status&type!=Item'
        '&searchTerm=my+favorite+experiment&searchTerm=my+other+experiment'
        '&searchTerm!=whatever'
    )
    p = ParamsParser(dummy_request)
    assert p.get_search_term_filters() == [
        ('searchTerm', 'my favorite experiment'),
        ('searchTerm', 'my other experiment'),
        ('searchTerm!', 'whatever')
    ]


def test_searches_parsers_params_parser_is_param(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&files.file_type=fastq&field=status'
    )
    p = ParamsParser(dummy_request)
    assert p.is_param(key='type', value='File')
    assert p.is_param(key='files.file_type', value='fastq')
    assert not p.is_param(key='files.file_type', value='bam')


def test_searches_parsers_params_parser_get_must_match_filter(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&type=File&files.file_type!=fastq&field=status'
    )
    p = ParamsParser(dummy_request)
    assert p.get_must_match_filters() == [
        ('type', 'Experiment'),
        ('type', 'File'),
        ('field', 'status')
    ]


def test_searches_parsers_params_parser_get_must_not_match_filter(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type!=Experiment&type=File&files.file_type=fastq&field=status'
    )
    p = ParamsParser(dummy_request)
    assert p.get_must_not_match_filters() == [
        ('type!', 'Experiment')
    ]


def test_searches_parsers_params_parser_chain_filters(dummy_request):
    from snovault.searches.parsers import ParamsParser
    dummy_request.environ['QUERY_STRING'] = (
        'type!=Experiment&type=File&files.file_type=fastq&field!=status'
    )
    p = ParamsParser(dummy_request)
    assert p.get_type_filters(params=p.get_must_not_match_filters()) == [
        ('type!', 'Experiment')
    ]
    assert p.get_must_not_match_filters(params=p.get_type_filters()) == [
        ('type!', 'Experiment')
    ]
