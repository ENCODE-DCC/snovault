# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook


def test_search_view(workbook, testapp):
    res = testapp.get('/search/?type=Item').json
    assert res['@type'] == ['Search']
    assert res['@id'] == '/search/?type=Item'
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
    # the following line fails cause we don't support rev_linked items
    #test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001RIC']
    test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001MXD']
    assert test_json[0]['lab']['uuid'] == 'cfb789b8-46f3-4d59-a2b3-adc39e7df93a'
    # this specific field should be embedded ('lab.awards.project')
    assert test_json[0]['lab']['awards'][0]['project'] == 'ENCODE'
    # this specific field should be embedded ('lab.awards.title')
    assert test_json[0]['lab']['awards'][0]['title'] == 'A DATA COORDINATING CENTER FOR ENCODE'
    # this specific field was not embedded and should not be present
    assert 'name' not in test_json[0]['lab']['awards'][0]
    # the whole award object should be embedded.
    # test type and a couple keys
    assert isinstance(test_json[0]['award'], dict)
    assert test_json[0]['award']['start_date'] == '2012-09-21'
    # since award.pi was not specifically embedded, pi field should not exist
    # (removed @id-like field)
    assert 'pi' not in test_json[0]['award']
    # @id-like field that should still be embedded (not a valid @id)
    assert test_json[0]['lab']['city'] == 'Stanford/USA/'
