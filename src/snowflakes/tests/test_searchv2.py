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


def test_searchv2_view_values(workbook, testapp):
    r = testapp.get(
        '/searchv2/?status=released'
    )
    assert r.json['all'] == '/searchv2/?status=released&limit=all'
    assert r.json['notification'] == 'Success'
    assert r.json['filters'][0] == {'field': 'status', 'remove': '/searchv2/', 'term': 'released'}


def test_searchv2_view_values_no_results(workbook, testapp):
    r = testapp.get(
        '/searchv2/?status=released&?type=Sno',
        status=404
    )
    assert r.json['notification'] == 'No results found'
    assert r.status_code == 404


def test_searchv2_view_no_type(workbook, testapp):
    r = testapp.get('/searchv2/')
    assert 'total' in r.json
    assert 'filters' in r.json
    assert len(r.json['filters']) == 0


def test_searchv2_view_raw_response(workbook, testapp):
    r = testapp.get('/searchv2_raw/?type=Snowflake')
    assert 'hits' in r.json
