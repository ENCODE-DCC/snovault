# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook


def test_search_view(workbook, testapp):
    res = testapp.get('/search/').json
    assert res['@type'] == ['Search']
    assert res['@id'] == '/search/'
    assert res['@context'] == '/terms/'
    assert res['notification'] == 'Success'
    assert res['title'] == 'Search'
    assert res['total'] > 0
    assert 'facets' in res
    assert 'filters' in res
    assert 'columns' in res
    assert '@graph' in res


def test_selective_embedding(workbook, testapp):
    res = testapp.get('/search/?type=Snowflake').json
    # Use a specific snowflake, found by accession from test data
    # Check the embedding /types/snow.py entry for Snowflakes; test ensures
    # that the actual embedding matches that
    test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001RIC']
    assert test_json[0]['lab']['uuid'] == 'cfb789b8-46f3-4d59-a2b3-adc39e7df93a'
    assert test_json[0]['lab']['awards'][0]['project'] == 'ENCODE'


def test_report_view(workbook, testapp):
    res = testapp.get('/report/?type=Lab').json
    assert res['@type'] == ['Report']
    assert res['@id'] == '/report/?type=Lab'
    assert res['@context'] == '/terms/'
    assert res['notification'] == 'Success'
    assert res['title'] == 'Report'
    assert res['total'] > 0
    assert 'facets' in res
    assert 'filters' in res
    assert 'columns' in res
    assert '@graph' in res
