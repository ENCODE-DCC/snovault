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


def test_searches_queries_abstract_query_init():
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery({})
    assert isinstance(aq, AbstractQuery)


def test_searches_queries_abstract_query_get_or_create_search(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    from elasticsearch_dsl import Search
    aq = AbstractQuery(params_parser)
    assert aq.search is None
    s = aq._get_or_create_search()
    assert s is aq.search
    assert isinstance(aq.search, Search)


def test_searches_queries_abstract_query_get_client(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    from elasticsearch import Elasticsearch
    aq = AbstractQuery(params_parser, client={'a': 'client'})
    c = aq._get_client()
    assert c == {'a': 'client'}
    aq = AbstractQuery(params_parser)
    c = aq._get_client()
    assert isinstance(c, Elasticsearch)


def test_searches_queries_abstract_query_get_index(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    from snovault.elasticsearch.interfaces import RESOURCES_INDEX
    aq = AbstractQuery(params_parser)
    assert aq._get_index() == RESOURCES_INDEX


def test_searches_queries_abstract_query_get_doc_types(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    doc_types = aq._get_doc_types()
    assert doc_types == [
        ('type', 'Experiment')
    ]


def test_searches_queries_abstract_query_get_query(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    search_terms = aq.params_parser.param_values_to_list(aq._get_query())
    assert search_terms == [
        'chip-seq'
    ]


def test_searches_queries_abstract_query_get_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
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


def test_searches_queries_abstract_query_get_post_filters(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    assert aq._get_post_filters() is None


def test_searches_queries_abstract_query_get_sort(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    sort_by = aq._get_sort()
    assert sort_by == [
        ('sort', 'date_created'),
        ('sort', '-files.file_size')
    ]


def test_searches_queries_abstract_query_get_limit(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    limit = aq.params_parser.param_values_to_list(aq._get_limit())
    assert limit == [
        '10'
    ]


def test_searches_queries_abstract_query_get_search_fields(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    assert aq._get_search_fields() is None


def test_searches_queries_abstract_query_get_return_fields(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    return_fields = aq.params_parser.param_values_to_list(aq._get_return_fields())
    assert return_fields == [
        '@id',
        'accession'
    ]


def test_searches_queries_abstract_query_get_facets(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    assert aq._get_facets() is None


def test_searches_queries_abstract_query_get_facet_size(params_parser):
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery(params_parser)
    assert aq._get_facet_size() is None


def test_searches_queries_abstract_query_add_query(params_parser):
    assert False 


def test_searches_queries_abstract_query_add_filters(params_parser):
    assert False 


def test_searches_queries_abstract_query_add_aggs(params_parser):
    assert False 


def test_searches_queries_abstract_query_build_query():
    from snovault.elasticsearch.searches.queries import AbstractQuery
    aq = AbstractQuery({})
    with pytest.raises(NotImplementedError):
        aq.build_query()


def test_searches_builders_basic_search_query_builder_init(params_parser):
    from snovault.elasticsearch.searches.queries import BasicSearchQuery
    bsq = BasicSearchQuery(params_parser)
    assert isinstance(bsq, BasicSearchQuery)
    assert bsq.params_parser == params_parser
