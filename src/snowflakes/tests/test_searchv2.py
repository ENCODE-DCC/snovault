import pytest

# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook
from webob.multidict import MultiDict


def test_searchv2_view(workbook, testapp):
    r = testapp.get('/searchv2/?type=Snowflake')
    print(r.json)
    assert False


def test_searchv2_view_facets(workbook, testapp):
    r = testapp.get('/searchv2/?type=Snowflake')
    print(r.json['facets'])
    assert False


def test_searchv2_view_no_type(workbook, testapp):
    r = testapp.get('/searchv2/')
    print(r.json)
    assert False


def test_searchv2_view_raw_response(workbook, testapp):
    r = testapp.get('/searchv2_raw/?type=Snowflake')
    print(r.json)
    assert False
