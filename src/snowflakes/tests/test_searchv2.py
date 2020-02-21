import pytest

# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook
from webob.multidict import MultiDict


def test_searchv2_view(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    )
    assert r.json['title'] == 'Search'
    assert len(r.json['@graph']) == 1
    assert r.json['@graph'][0]['accession'] == 'SNOFL000LSQ'
    assert r.json['@graph'][0]['status'] == 'deleted'
    assert 'Snowflake' in r.json['@graph'][0]['@type']
    assert len(r.json['facets']) == 5
    assert r.json['@id'] == '/search/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    assert r.json['@context'] == '/terms/'
    assert r.json['@type'] == ['Search']
    assert r.json['total'] == 1
    assert r.json['notification'] == 'Success'
    assert len(r.json['filters']) == 4
    assert r.status_code == 200
    assert r.json['clear_filters'] == '/search/?type=Snowflake'
    assert 'debug' not in r.json
    assert 'columns' in r.json
    assert 'sort' in r.json


def test_searchv2_view_with_limit(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&limit=5'
    )
    assert len(r.json['@graph']) == 5
    assert 'all' in r.json
    r = testapp.get(
        '/search/?type=Snowflake&limit=26'
    )
    assert len(r.json['@graph']) == 26
    assert 'all' in r.json
    r = testapp.get(
        '/search/?type=Snowflake&limit=all'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/search/?type=Snowflake&limit=35'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/search/?type=Snowflake&limit=100000'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json


def test_searchv2_view_with_limit_and_scan(workbook, testapp, mocker):
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    mocker.patch.object(BasicSearchQueryFactoryWithFacets, '_should_scan_over_results')
    BasicSearchQueryFactoryWithFacets._should_scan_over_results.return_value = True
    r = testapp.get(
        '/search/?type=Snowflake&limit=all'
    )
    assert len(r.json['@graph']) == 35
    r = testapp.get(
        '/search/?type=Snowflake&limit=30'
    )
    assert len(r.json['@graph']) == 30
    BasicSearchQueryFactoryWithFacets._should_scan_over_results.return_value = False
    r = testapp.get(
        '/search/?type=Snowflake&limit=all'
    )
    assert len(r.json['@graph']) == 25
    r = testapp.get(
        '/search/?type=Snowflake&limit=30'
    )
    assert len(r.json['@graph']) == 30


def test_searchv2_view_with_limit_zero(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&limit=0'
    )
    assert len(r.json['@graph']) == 0
    assert 'all' in r.json
    assert r.json['total'] == 35


def test_searchv2_view_values(workbook, testapp):
    r = testapp.get(
        '/search/?status=released'
    )
    assert r.json['all'] == '/search/?status=released&limit=all'
    assert r.json['notification'] == 'Success'
    assert r.json['filters'][0] == {'field': 'status', 'remove': '/search/', 'term': 'released'}
    assert r.json['clear_filters'] == '/search/'


def test_searchv2_view_values_no_results(workbook, testapp):
    r = testapp.get(
        '/search/?status=current&type=Snowflake',
        status=404
    )
    assert r.json['notification'] == 'No results found'


def test_searchv2_view_values_malformed_query_string(workbook, testapp):
    r = testapp.get(
        '/search/?status=current&type=Snowflake&status=&format=json',
        status=404
    )
    assert r.json['notification'] == 'No results found'


def test_searchv2_view_values_regex_slash_escape(workbook, testapp):
    r = testapp.get(
        '/search/?searchTerm=/*'
    )
    assert r.json['total'] >= 32


def test_searchv2_view_values_bad_type(workbook, testapp):
    r = testapp.get(
        '/search/?status=released&type=Sno',
        status=400
    )
    assert r.json['description'] == "Invalid types: ['Sno']"
    r = testapp.get(
        '/search/?status=released&type=Sno&type=Flake',
        status=400
    )
    assert r.json['description'] == "Invalid types: ['Sno', 'Flake']"


def test_searchv2_view_values_item_wildcard(workbook, testapp):
    r = testapp.get(
        '/search/?type=*',
    )
    assert r.json['notification'] == 'Success'
    assert r.json['total'] >= 172


def test_searchv2_view_values_invalid_search_term(workbook, testapp):
    r = testapp.get(
        '/search/?searchTerm=[',
        status=404
    )
    r = testapp.get(
        '/search/?searchTerm=cherry^',
        status=200
    )
    assert r.json['total'] == 1


def test_searchv2_view_values_invalid_advanced_query(workbook, testapp):
    r = testapp.get(
        '/search/?advancedQuery=[',
        status=400
    )
    assert r.json['description'] == 'Invalid query: ([)'


def test_searchv2_view_values_reserved_characters_advanced_query(workbook, testapp):
    r = testapp.get(
        '/search/?searchTerm=cherry^',
        status=200
    )
    assert r.json['total'] == 1
    r = testapp.get(
        '/search/?searchTerm=cherry~',
        status=200
    )
    assert r.json['total'] == 1
    r = testapp.get(
        '/search/?searchTerm=/cherry',
        status=200
    )
    assert r.json['total'] == 1
    r = testapp.get(
        '/search/?searchTerm=/cherryp',
        status=404
    )
    assert r.json['total'] == 0
    r = testapp.get(
        '/search/?searchTerm=/cherry:',
        status=200
    )
    assert r.json['total'] == 1


def test_searchv2_view_embedded_frame(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&frame=embedded'
    )
    assert r.json['@graph'][0]['lab']['name']


def test_searchv2_view_object_frame(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&frame=object'
    )
    res = r.json['@graph'][0]
    assert all(
        [
            x in res
            for x in ['accession', '@type', '@id', 'status']
        ]
    )

def test_searchv2_view_debug_query(workbook, testapp):
    r = testapp.get(
        '/search/?type=Snowflake&debug=true'
    )
    assert 'debug' in r.json
    assert 'post_filter' in r.json['debug']['raw_query']


def test_searchv2_view_no_type(workbook, testapp):
    r = testapp.get('/search/')
    assert 'total' in r.json
    assert 'filters' in r.json
    assert len(r.json['filters']) == 0


def test_searchv2_view_no_type_debug(workbook, testapp):
    r = testapp.get('/search/?debug=true')
    assert not r.json['debug']['raw_query']['post_filter']['bool']


def test_searchv2_raw_view_raw_response(workbook, testapp):
    r = testapp.get('/searchv2_raw/?type=Snowflake')
    assert 'hits' in r.json
    assert 'aggregations' in r.json
    assert '_shards' in r.json
    assert 'timed_out' in r.json
    assert 'took' in r.json


def test_searchv2_raw_view_raw_response_limit_all(workbook, testapp):
    r = testapp.get('/searchv2_raw/?type=*&limit=all')
    assert 'hits' in r.json
    assert len(r.json['hits']['hits']) > 170


def test_searchv2_quick_view_limit_all(workbook, testapp):
    r = testapp.get('/searchv2_quick/?type=*&limit=all')
    assert '@graph' in r.json
    assert len(r.json['@graph']) > 170
    assert '@id' in r.json['@graph'][0]
    assert len(r.json.keys()) == 1


def test_searchv2_quick_view_one_item(workbook, testapp):
    r = testapp.get(
        '/searchv2_quick/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    )
    assert len(r.json['@graph']) == 1


def test_searchv2_quick_view_specify_field(workbook, testapp):
    r = testapp.get('/searchv2_quick/?type=Snowflake&field=@id')
    assert '@id' in r.json['@graph'][0]
    assert '@type' in r.json['@graph'][0]
    assert len(r.json['@graph'][0].keys()) == 2


def test_search_generator(workbook, threadlocals, dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    from types import GeneratorType
    dummy_request.environ['QUERY_STRING'] = (
        'type=*&limit=all'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch(port=9201)
    from snowflakes.search_views import search_generator
    r = search_generator(dummy_request)
    assert '@graph' in r
    assert len(r.keys()) == 1
    assert isinstance(r['@graph'], GeneratorType)
    hits = [dict(h) for h in r['@graph']]
    assert len(hits) > 150
    assert '@id' in hits[0]


def test_search_generator_field_specified(workbook, threadlocals, dummy_request):
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch import ELASTIC_SEARCH
    from elasticsearch import Elasticsearch
    from types import GeneratorType
    dummy_request.environ['QUERY_STRING'] = (
        'type=Snowflake&field=@id&limit=5'
    )
    dummy_request.registry[ELASTIC_SEARCH] = Elasticsearch(port=9201)
    from snowflakes.search_views import search_generator
    r = search_generator(dummy_request)
    assert '@graph' in r
    assert len(r.keys()) == 1
    assert isinstance(r['@graph'], GeneratorType)
    hits = [dict(h) for h in r['@graph']]
    assert len(hits) == 5
    assert '@id' in hits[0]
    assert len(hits[0].keys()) == 2


def test_reportv2_view(workbook, testapp):
    r = testapp.get(
        '/report/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    )
    assert r.json['title'] == 'Report'
    assert len(r.json['@graph']) == 1
    assert r.json['@graph'][0]['accession'] == 'SNOFL000LSQ'
    assert r.json['@graph'][0]['status'] == 'deleted'
    assert 'Snowflake' in r.json['@graph'][0]['@type']
    assert len(r.json['facets']) == 5
    assert r.json['@id'] == '/report/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    assert r.json['@context'] == '/terms/'
    assert r.json['@type'] == ['Report']
    assert r.json['total'] == 1
    assert r.json['notification'] == 'Success'
    assert len(r.json['filters']) == 4
    assert r.status_code == 200
    assert r.json['clear_filters'] == '/report/?type=Snowflake'
    assert 'debug' not in r.json
    assert 'columns' in r.json
    assert 'non_sortable' in r.json
    assert 'sort' in r.json


def test_reportv2_response_with_search_term_type_only_clear_filters(workbook, testapp):
    r = testapp.get('/report/?type=Snowball&searchTerm=crisp')
    assert 'clear_filters' in r.json
    assert r.json['clear_filters'] == '/report/?type=Snowball'


def test_reportv2_view_with_limit(workbook, testapp):
    r = testapp.get(
        '/report/?type=Snowflake&limit=5'
    )
    assert len(r.json['@graph']) == 5
    assert 'all' in r.json
    r = testapp.get(
        '/report/?type=Snowflake&limit=26'
    )
    assert len(r.json['@graph']) == 26
    assert 'all' in r.json
    r = testapp.get(
        '/report/?type=Snowflake&limit=all'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/report/?type=Snowflake&limit=35'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/report/?type=Snowflake&limit=100000'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json


def test_reportv2_view_with_limit_zero(workbook, testapp):
    r = testapp.get(
        '/report/?type=Snowflake&limit=0'
    )
    assert len(r.json['@graph']) == 0
    assert 'all' in r.json
    assert r.json['total'] == 35


def test_reportv2_view_with_limit_zero_from_zero(workbook, testapp):
    r = testapp.get(
        '/report/?type=Snowflake&limit=0&from=0'
    )
    assert len(r.json['@graph']) == 0
    assert 'all' in r.json
    assert r.json['total'] == 35


def test_reportv2_view_values_bad_type(workbook, testapp):
    r = testapp.get(
        '/report/?status=released&type=Sno',
        status=400
    )
    assert r.json['description'] == "Invalid types: ['Sno']"
    r = testapp.get(
        '/report/?status=released&type=Sno&type=Flake',
        status=400
    )
    assert r.json['description'] == "Report view requires specifying a single type: [('type', 'Sno'), ('type', 'Flake')]"


def test_reportv2_view_values_single_subtype(workbook, testapp):
    r = testapp.get(
        '/report/?status=released&type=Item',
        status=400
    )
    assert 'Report view requires a type with no child types:' in r.json['description']


def test_reportv2_view_values_no_type(workbook, testapp):
    r = testapp.get(
        '/report/?status=released',
        status=400
    )
    assert r.json['description'] == 'Report view requires specifying a single type: []'


def test_matrixv2_raw_view_raw_response(workbook, testapp):
    r = testapp.get('/matrixv2_raw/?type=Snowball')
    assert 'hits' in r.json
    assert r.json['hits']['total'] >= 22
    assert len(r.json['hits']['hits']) == 0
    assert 'aggregations' in r.json
    assert 'x' in r.json['aggregations']
    assert 'y' in r.json['aggregations']
    assert 'snowflakes.type' in r.json['aggregations']['x']
    assert 'award.rfa' in r.json['aggregations']['y']
    assert '_shards' in r.json
    assert 'timed_out' in r.json
    assert 'took' in r.json


def test_matrixv2_raw_view_no_matrix_defined(workbook, testapp):
    r = testapp.get(
        '/matrixv2_raw/?type=Snowflake',
        status=400
    )
    assert r.json['description'] == 'Item type does not have requested view defined: {}'


def test_matrixv2_raw_view_invalid_type(workbook, testapp):
    r = testapp.get(
        '/matrixv2_raw/?type=Sno',
        status=400
    )
    assert r.json['description'] == "Invalid types: ['Sno']"


def test_matrixv2_raw_view_mutiple_types(workbook, testapp):
    r = testapp.get(
        '/matrixv2_raw/?type=Snowflake&type=Snowball',
        status=400
    )
    assert 'Matrix view requires specifying a single type' in r.json['description']


def test_matrixv2_raw_view_no_types(workbook, testapp):
    r = testapp.get(
        '/matrixv2_raw/',
        status=400
    )
    assert r.json['description'] == 'Matrix view requires specifying a single type: []'


def test_matrixv2_response(workbook, testapp):
    r = testapp.get('/matrix/?type=Snowball')
    assert 'aggregations' not in r.json
    assert 'facets' in r.json
    assert 'total' in r.json
    assert r.json['title'] == 'Matrix'
    assert r.json['@type'] == ['Matrix']
    assert r.json['clear_filters'] == '/matrix/?type=Snowball'
    assert r.json['filters'] == [{'term': 'Snowball', 'remove': '/matrix/', 'field': 'type'}]
    assert r.json['@id'] == '/matrix/?type=Snowball'
    assert r.json['total'] >= 22
    assert r.json['notification'] == 'Success'
    assert r.json['title'] == 'Matrix'
    assert 'facets' in r.json
    assert r.json['@context'] == '/terms/'
    assert 'matrix' in r.json
    assert r.json['matrix']['x']['group_by'] == 'snowflakes.type'
    assert r.json['matrix']['y']['group_by'] == ['award.rfa', 'lab.title']
    assert 'buckets' in r.json['matrix']['y']['award.rfa']
    assert 'key' in r.json['matrix']['y']['award.rfa']['buckets'][0]
    assert 'lab.title' in r.json['matrix']['y']['award.rfa']['buckets'][0]
    assert 'search_base' in r.json
    assert r.json['search_base'] == '/search/?type=Snowball'


def test_matrixv2_response_with_search_term_type_only_clear_filters(workbook, testapp):
    r = testapp.get('/matrix/?type=Snowball&searchTerm=crisp')
    assert 'clear_filters' in r.json
    assert r.json['clear_filters'] == '/matrix/?type=Snowball'


def test_matrixv2_response_debug(workbook, testapp):
    r = testapp.get('/matrix/?type=Snowball&debug=true')
    assert 'debug' in r.json


def test_matrixv2_response_no_results(workbook, testapp):
    r = testapp.get(
        '/matrix/?type=Snowball&status=no_status',
        status=404
    )
    assert r.json['notification'] == 'No results found'


def test_missing_matrix_response(workbook, testapp):
    r = testapp.get('/missing_matrix/?type=Snowball')
    assert 'aggregations' not in r.json
    assert 'facets' in r.json
    assert 'total' in r.json
    assert r.json['title'] == 'Matrix'
    assert r.json['@type'] == ['Matrix']
    assert r.json['clear_filters'] == '/missing_matrix/?type=Snowball'
    assert r.json['filters'] == [{'term': 'Snowball', 'remove': '/missing_matrix/', 'field': 'type'}]
    assert r.json['@id'] == '/missing_matrix/?type=Snowball'
    assert r.json['total'] >= 22
    assert r.json['notification'] == 'Success'
    assert r.json['title'] == 'Matrix'
    assert 'facets' in r.json
    assert r.json['@context'] == '/terms/'
    assert 'matrix' in r.json
    assert r.json['matrix']['x']['group_by'] == 'snowflakes.type'
    assert r.json['matrix']['y']['group_by'] == ['award.rfa', ['lab.not_a_real_value', 'some_lab']]
    assert 'buckets' in r.json['matrix']['y']['award.rfa']
    assert 'key' in r.json['matrix']['y']['award.rfa']['buckets'][0]
    assert 'lab.not_a_real_value' in r.json['matrix']['y']['award.rfa']['buckets'][0]
    assert r.json['matrix']['y']['award.rfa']['buckets'][0]['lab.not_a_real_value']['buckets'][0]['key'] == 'some_lab'
    assert 'search_base' in r.json
    assert r.json['search_base'] == '/search/?type=Snowball'


def test_summaryv2_response(workbook, testapp):
    r = testapp.get('/summary/?type=Snowball')
    assert 'aggregations' not in r.json
    assert 'facets' in r.json
    assert 'total' in r.json
    assert r.json['title'] == 'Summary'
    assert r.json['@type'] == ['Summary']
    assert r.json['clear_filters'] == '/summary/?type=Snowball'
    assert r.json['filters'] == [{'term': 'Snowball', 'remove': '/summary/', 'field': 'type'}]
    assert r.json['@id'] == '/summary/?type=Snowball'
    assert r.json['total'] >= 22
    assert r.json['notification'] == 'Success'
    assert r.json['title'] == 'Summary'
    assert 'facets' in r.json
    assert r.json['@context'] == '/terms/'
    assert 'matrix' in r.json
    assert r.json['matrix']['x']['group_by'] == 'status'
    assert r.json['matrix']['y']['group_by'] == ['snowflakes.type']
    assert 'buckets' in r.json['matrix']['y']['snowflakes.type']
    assert 'key' in r.json['matrix']['y']['snowflakes.type']['buckets'][0]
    assert 'status' in r.json['matrix']['y']['snowflakes.type']['buckets'][0]
    assert 'search_base' in r.json
    assert r.json['search_base'] == '/search/?type=Snowball'


def test_collection_listing_es_view(workbook, testapp):
    r = testapp.get(
        '/snowflakes/'
    )
    assert '@graph' in r.json
    assert '@id' in r.json
    assert 'facets' in r.json
    assert 'filters' in r.json
    assert 'all' in r.json
    assert 'columns' in r.json
    assert 'clear_filters' in r.json
    assert r.json['clear_filters'] == '/search/?type=Snowflake'
    assert r.json['@type'] == ['SnowflakeCollection', 'Collection']
    assert r.json['@context'] == '/terms/'


def test_collection_listing_es_view_item(workbook, testapp):
    r = testapp.get(
        '/Snowflake'
    )
    r = r.follow()
    assert '@graph' in r.json
    assert '@id' in r.json
    assert 'facets' in r.json
    assert 'filters' in r.json
    assert r.json['@type'] == ['SnowflakeCollection', 'Collection']
    assert r.json['@context'] == '/terms/'


def test_collection_listing_db_view(workbook, testapp):
    r = testapp.get(
        '/snowflakes/?datastore=database'
    )
    assert '@graph' in r.json
    assert '@id' in r.json
    assert 'facets' not in r.json
    assert 'filters' not in r.json
    assert r.json['@type'] == ['SnowflakeCollection', 'Collection']
    assert r.json['@context'] == '/terms/'
    assert r.json['description'] == 'Listing of Snowflakes'


def test_auditv2_response(workbook, testapp):
    r = testapp.get('/audit/?type=Snowball')
    assert 'aggregations' not in r.json
    assert 'facets' in r.json
    assert 'total' in r.json
    assert r.json['title'] == 'Audit'
    assert r.json['@type'] == ['Audit']
    assert r.json['clear_filters'] == '/audit/?type=Snowball'
    assert r.json['filters'] == [{'term': 'Snowball', 'remove': '/audit/', 'field': 'type'}]
    assert r.json['@id'] == '/audit/?type=Snowball'
    assert r.json['total'] >= 22
    assert r.json['notification'] == 'Success'
    assert 'facets' in r.json
    assert r.json['@context'] == '/terms/'
    assert 'matrix' in r.json
    assert r.json['matrix']['x']['group_by'] == 'snowflakes.type'
    assert r.json['matrix']['audit.ERROR.category']
    assert r.json['matrix']['audit.WARNING.category']
    assert r.json['matrix']['audit.NOT_COMPLIANT.category']
    assert r.json['matrix']['audit.INTERNAL_ACTION.category']
    assert 'search_base' in r.json
    assert r.json['search_base'] == '/search/?type=Snowball'


def test_auditv2_response_with_search_term_type_only_clear_filters(workbook, testapp):
    r = testapp.get('/audit/?type=Snowball&searchTerm=crisp')
    assert 'clear_filters' in r.json
    assert r.json['clear_filters'] == '/audit/?type=Snowball'


def test_auditv2_response_debug(workbook, testapp):
    r = testapp.get('/audit/?type=Snowball&debug=true')
    assert 'debug' in r.json


def test_auditv2_response_no_results(workbook, testapp):
    r = testapp.get(
        '/audit/?type=Snowball&status=no_status',
        status=404
    )
    assert r.json['notification'] == 'No results found'


def test_auditv2_view_no_matrix_defined(workbook, testapp):
    r = testapp.get(
        '/audit/?type=Snowflake',
        status=400
    )
    assert r.json['description'] == 'Item type does not have requested view defined: {}'
