import pytest

# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook
from webob.multidict import MultiDict


def test_searchv2_view(workbook, testapp):
    r = testapp.get('/search/?type=Snowflake')
    print(r.json)
    assert False
