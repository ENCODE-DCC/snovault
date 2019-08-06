import pytest

# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook
from webob.multidict import MultiDict


def test_searchv2_view(workbook, testapp):
    r = testapp.get(
        '/searchv2/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    )
    assert r.json['title'] == 'Search'
    assert len(r.json['@graph']) == 1
    assert r.json['@graph'][0]['accession'] == 'SNOFL000LSQ'
    assert r.json['@graph'][0]['status'] == 'deleted'
    assert 'Snowflake' in r.json['@graph'][0]['@type']
    assert len(r.json['facets']) == 5
    assert r.json['@id'] == '/searchv2/?type=Snowflake&award=/awards/U41HG006992/&accession=SNOFL000LSQ&status=deleted'
    assert r.json['@context'] == '/terms/'
    assert r.json['@type'] == ['Search']
    assert r.json['total'] == 1
    assert r.json['notification'] == 'Success'
    assert len(r.json['filters']) == 4
    assert r.status_code == 200
    assert r.json['clear_filters'] == '/searchv2/?type=Snowflake'
    assert 'debug' not in r.json


def test_searchv2_view_with_limit(workbook, testapp):
    r = testapp.get(
        '/searchv2/?type=Snowflake&limit=5'
    )
    assert len(r.json['@graph']) == 5
    assert 'all' in r.json
    r = testapp.get(
        '/searchv2/?type=Snowflake&limit=26'
    )
    assert len(r.json['@graph']) == 26
    assert 'all' in r.json
    r = testapp.get(
        '/searchv2/?type=Snowflake&limit=all'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/searchv2/?type=Snowflake&limit=35'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json
    r = testapp.get(
        '/searchv2/?type=Snowflake&limit=100000'
    )
    assert len(r.json['@graph']) == 35
    assert 'all' not in r.json


def test_searchv2_view_values(workbook, testapp):
    r = testapp.get(
        '/searchv2/?status=released'
    )
    assert r.json['all'] == '/searchv2/?status=released&limit=all'
    assert r.json['notification'] == 'Success'
    assert r.json['filters'][0] == {'field': 'status', 'remove': '/searchv2/', 'term': 'released'}
    assert r.json['clear_filters'] == '/searchv2/'


def test_searchv2_view_values_no_results(workbook, testapp):
    r = testapp.get(
        '/searchv2/?status=current&type=Snowflake',
        status=404
    )
    assert r.json['notification'] == 'No results found'


def test_searchv2_view_values_bad_type(workbook, testapp):
    r = testapp.get(
        '/searchv2/?status=released&type=Sno',
        status=400
    )
    assert r.json['description'] == "Invalid types: ['Sno']"
    r = testapp.get(
        '/searchv2/?status=released&type=Sno&type=Flake',
        status=400
    )
    
    assert r.json['description'] == "Invalid types: ['Sno', 'Flake']"


def test_searchv2_view_values_item_wildcard(workbook, testapp):
    r = testapp.get(
        '/searchv2/?type=*',
    )
    assert r.json['notification'] == 'Success'
    assert r.json['total'] >= 172


def test_searchv2_view_values_invalid_search_term(workbook, testapp):
    r = testapp.get(
        '/searchv2/?searchTerm=[',
        status=400
    )
    assert r.json['description'] == 'Invalid query: ([)'


def test_searchv2_view_values_invalid_advanced_query(workbook, testapp):
    r = testapp.get(
        '/searchv2/?advancedQuery=[',
        status=400
    )
    assert r.json['description'] == 'Invalid query: ([)'


def test_searchv2_view_embedded_frame(workbook, testapp):
    r = testapp.get(
        '/searchv2/?type=Snowflake&frame=embedded'
    )
    assert r.json['@graph'][0]['lab']['name']


def test_searchv2_view_object_frame(workbook, testapp):
    r = testapp.get(
        '/searchv2/?type=Snowflake&frame=object'
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
        '/searchv2/?type=Snowflake&debug=true'
    )
    assert 'debug' in r.json
    assert 'post_filter' in r.json['debug']['raw_query']


def test_searchv2_view_no_type(workbook, testapp):
    r = testapp.get('/searchv2/')
    assert 'total' in r.json
    assert 'filters' in r.json
    assert len(r.json['filters']) == 0


def test_searchv2_view_raw_response(workbook, testapp):
    r = testapp.get('/searchv2_raw/?type=Snowflake')
    assert 'hits' in r.json
