import pytest


@pytest.fixture
def params_parser(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
        '&biosample_ontology.term_name%21=naive+thymus-derived+CD4-positive%2C+alpha-beta+T+cell'
        '&limit=10&status=released&searchTerm=chip-seq&sort=date_created&sort=-files.file_size'
        '&field=@id&field=accession'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    return ParamsParser(dummy_request)


@pytest.fixture
def params_parser_snovault_types(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=released'
        '&limit=10&field=@id&field=accession'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch()
    return ParamsParser(dummy_request)


def test_searches_queries_abstract_query_factory_init():
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory({})
    assert isinstance(aq, AbstractQueryFactory)


def test_searches_queries_abstract_query_factory_get_or_create_search(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from elasticsearch_dsl import Search
    aq = AbstractQueryFactory(params_parser)
    assert aq.search is None
    s = aq._get_or_create_search()
    assert s is aq.search
    assert isinstance(aq.search, Search)


def test_searches_queries_abstract_query_factory_get_client(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from elasticsearch import Elasticsearch
    aq = AbstractQueryFactory(params_parser, client={'a': 'client'})
    c = aq._get_client()
    assert c == {'a': 'client'}
    aq = AbstractQueryFactory(params_parser)
    c = aq._get_client()
    assert isinstance(c, Elasticsearch)


def test_searches_queries_abstract_query_factory_get_index(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    from snovault.elasticsearch.interfaces import RESOURCES_INDEX
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_index() == RESOURCES_INDEX


def test_searches_queries_abstract_query_factory_get_item_types(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    item_types = aq._get_item_types()
    assert item_types == [
        ('type', 'Experiment')
    ]


def test_searches_queries_abstract_query_factory_get_default_item_types(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(
        params_parser,
        default_item_types=[
            'Snowflake',
            'Pancake'
        ]
    )
    default_item_types = aq._get_default_item_types()
    assert default_item_types == [
        'Snowflake',
        'Pancake'
    ]


def test_searches_queries_abstract_query_factory_get_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    search_terms = aq._get_query()
    assert search_terms == '(chip-seq)'


def test_searches_queries_abstract_query_factory_get_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    filters = aq._get_filters()
    assert filters == [
        ('assay_title', 'Histone ChIP-seq'),
        ('award.project', 'Roadmap'),
        ('assembly', 'GRCh38'),
        ('biosample_ontology.classification', 'primary cell'),
        ('target.label', 'H3K27me3'),
        ('biosample_ontology.classification!', 'cell line'),
        ('biosample_ontology.term_name!', 'naive thymus-derived CD4-positive, alpha-beta T cell'),
        ('status', 'released')
    ]


def test_searches_queries_abstract_query_factory_get_post_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_post_filters() == []


def test_searches_queries_abstract_query_factory_get_sort(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    sort_by = aq._get_sort()
    assert sort_by == [
        ('sort', 'date_created'),
        ('sort', '-files.file_size')
    ]


def test_searches_queries_abstract_query_factory_get_limit(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    limit = aq.params_parser.param_values_to_list(aq._get_limit())
    assert limit == [
        '10'
    ]


def test_searches_queries_abstract_query_factory_get_search_fields(params_parser_snovault_types):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser_snovault_types)
    search_fields = aq._get_search_fields()
    assert all(
        f in search_fields
        for f in [
                'embedded.status',
                '*.md5sum',
                '_all',
                'unique_keys.*',
                'embedded.accession',
                '*.uuid',
                '*.submitted_file_name'
        ]
    )


def test_searches_queries_abstract_query_factory_get_return_fields(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    return_fields = aq.params_parser.param_values_to_list(aq._get_return_fields())
    assert return_fields == [
        '@id',
        'accession'
    ]


def test_searches_queries_abstract_query_factory_combine_search_term_queries(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&searchTerm=rna&searchTerm!=ENCODE+2'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == '(chip-seq) AND (rna) AND NOT (ENCODE 2)'
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == '(chip-seq)'
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm!=rna&searchTerm!=ENCODE+2'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms == 'NOT (rna) AND NOT (ENCODE 2)'
    dummy_request.environ['QUERY_STRING'] = (
        'type=Experiment'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    combined_search_terms = aq._combine_search_term_queries(
        must_match_filters=aq.params_parser.get_must_match_search_term_filters(),
        must_not_match_filters=aq.params_parser.get_must_not_match_search_term_filters()
    )
    assert combined_search_terms is None


def test_searches_queries_abstract_query_factory_get_facets(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_facets() == []


def test_searches_queries_abstract_query_factory_get_facet_size(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_facet_size() is None


def test_searches_queries_abstract_query_factory_get_boost_values_from_item_type(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._get_boost_values_from_item_type(
        'TestingSearchSchema'
    ) == {'accession': 1.0, 'status': 1.0}


def test_searches_queries_abstract_query_factory_prefix_values(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    assert aq._prefix_values(
        'embedded.',
        ['uuid', 'status', '@type']
    ) == ['embedded.uuid', 'embedded.status', 'embedded.@type']


def test_searches_queries_abstract_query_factory_add_query_string_query(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq._add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq&type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq._add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_with_default_type(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'searchTerm=chip-seq'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(
        params_parser,
        default_item_types=[
            'TestingSearchSchema'
        ]
    )
    aq._add_query_string_query()
    constructed_query = aq.search.to_dict()
    expected_query = {
        'query': {
            'query_string': {
                'default_operator': 'AND',
                'fields': [
                    '_all',
                    '*.uuid',
                    '*.md5sum',
                    '*.submitted_file_name',
                    'unique_keys.*',
                    'embedded.accession',
                    'embedded.status'
                ],
                'query': '(chip-seq)'
            }
        }
    }
    assert (
        constructed_query['query']['query_string']['query']
        == expected_query['query']['query_string']['query']
    )
    assert (
        constructed_query['query']['query_string']['default_operator']
        == expected_query['query']['query_string']['default_operator']
    )
    assert (
        set(constructed_query['query']['query_string']['fields'])
        == set(expected_query['query']['query_string']['fields'])
    )


def test_searches_queries_abstract_query_factory_add_query_string_query_no_search_term(dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&status=released'
    )
    params_parser = ParamsParser(dummy_request)
    aq = AbstractQueryFactory(params_parser)
    aq._add_query_string_query()
    assert aq.search is None


def test_searches_queries_abstract_query_factory_add_must_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'terms': {
                            'status': [
                                'released',
                                'archived'
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_must_not_equal_terms_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_must_not_equal_terms_filter(
        field='status',
        terms=['released', 'archived']
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'filter': [
                    {
                        'bool': {
                            'must_not': [
                                {
                                    'terms': {
                                        'status': [
                                            'released',
                                            'archived'
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    assert aq.search.to_dict() == {
        'query': {
            'exists': {
                'field': 'embedded.status'
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_exist_filter_multiple(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_exist_filter(
        'embedded.lab'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'must': [
                    {'exists': {'field': 'embedded.status'}},
                    {'exists': {'field': 'embedded.lab'}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'must_not': [
                    {'exists': {'field': 'embedded.file_size'}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_field_must_and_must_not_exist_filter(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_field_must_exist_filter(
        'embedded.status'
    )
    aq._add_field_must_not_exist_filter(
        'embedded.file_size'
    )
    assert aq.search.to_dict() == {
        'query': {
            'bool': {
                'must': [
                    {'exists': {'field': 'embedded.status'}}
                ],
                'must_not': [
                    {'exists': {'field': 'embedded.file_size'}}
                ]
            }
        }
    }


def test_searches_queries_abstract_query_factory_add_terms_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_terms_aggregation('Statuses', 'embedded.status', size=10)
    assert aq.search.to_dict() == {
        'aggs': {
            'Statuses': {
                'terms': {
                    'field': 'embedded.status',
                    'size': 10
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_add_exists_aggregation(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory(params_parser)
    aq._add_exists_aggregation('Processed file', 'embedded.derived_from', size=10)
    assert aq.search.to_dict() == {
        'aggs': {
            'Processed file': {
                'filters': {
                    'filters': {
                        'no': {
                            'bool': {
                                'must_not': [{'exists': {'field': 'embedded.derived_from'}}]
                            }
                        },
                        'yes': {
                            'exists': {
                                'field': 'embedded.derived_from'
                            }
                        }
                    }
                }
            }
        },
        'query': {'match_all': {}}
    }


def test_searches_queries_abstract_query_factory_add_filters(params_parser):
    assert False 


def test_searches_queries_abstract_query_factory_add_aggs(params_parser):
    assert False


def test_searches_queries_abstract_query_factory_add_source(params_parser):
    assert False 


def test_searches_queries_abstract_query_factory_build_query():
    from snovault.elasticsearch.searches.queries import AbstractQueryFactory
    aq = AbstractQueryFactory({})
    with pytest.raises(NotImplementedError):
        aq.build_query()


def test_searches_builders_basic_search_query_factory_init(params_parser):
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactory
    bsqf = BasicSearchQueryFactory(params_parser)
    assert isinstance(bsqf, BasicSearchQueryFactory)
    assert bsqf.params_parser == params_parser
