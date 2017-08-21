# Use workbook fixture from BDD tests (including elasticsearch)
from .features.conftest import app_settings, app, workbook
from webob.multidict import MultiDict
import pytest


def null_query():
    return {
        'query': {
            'bool': {
                'filter': [],
                'must_not': [],
                'must': [],
            },
        },
    }


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

# Unit tests


class FakeRequest(object):
    path = '/search/'

    def __init__(self, params):
        self.params = MultiDict(params)


def test_set_filters():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('field1', 'value1'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {'field1': ['value1']}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [
                    {
                        'terms': {
                            'embedded.field1': ['value1'],
                        },
                    },
                ],
            },
        },
            }
    assert result == {
        'filters': [
            {
                'field': 'field1',
                'term': 'value1',
                'remove': '/search/?'
            }
        ]
    }


def test_set_filters_searchTerm():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('searchTerm', 'value1'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [],
            },
        },
    }
    assert result == {
        'filters': [
            {
                'field': 'searchTerm',
                'term': 'value1',
                'remove': '/search/?'
            }
        ]
    }


# Reserved params should NOT act as filters
@pytest.mark.parametrize('param', [
    'type', 'limit', 'mode',
    'format', 'frame', 'datastore', 'field', 'sort', 'from', 'referrer'])
def test_set_filters_reserved_params(param):
    from snowflakes.search import set_filters

    request = FakeRequest((
        (param, 'foo'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [],
            },
        },
    }
    assert result == {
        'filters': [],
    }


def test_set_filters_multivalued():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('field1', 'value1'),
        ('field1', 'value2'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {'field1': ['value1', 'value2']}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [
                    {
                        'terms': {
                            'embedded.field1': ['value1', 'value2'],
                        },
                    },
                ],
            },
        },
    }
    assert result == {
        'filters': [
            {
                'field': 'field1',
                'term': 'value1',
                'remove': '/search/?field1=value2'
            },
            {
                'field': 'field1',
                'term': 'value2',
                'remove': '/search/?field1=value1'
            }
        ]
    }


def test_set_filters_negated():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('field1!', 'value1'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {'field1!': ['value1']}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [{'not':
                    {
                        'term': {'embedded.field1': ['value1']}
                    }
                }]
            }
        }
    }
    assert result == {
        'filters': [
            {
                'field': 'field1!',
                'term': 'value1',
                'remove': '/search/?'
            },
        ],
    }


def test_set_filters_audit():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('audit.foo', 'value1'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {'audit.foo': ['value1']}
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [
                    {
                        'terms': {
                            'audit.foo': ['value1'],
                        },
                    },
                ],
            },
        },
    }
    assert result == {
        'filters': [
            {
                'field': 'audit.foo',
                'term': 'value1',
                'remove': '/search/?'
            },
        ],
    }


def test_set_filters_exists_missing():
    from snowflakes.search import set_filters

    request = FakeRequest((
        ('field1', '*'),
        ('field2!', '*'),
    ))
    query = null_query()
    result = {'filters': []}
    used_filters = set_filters(request, query, result)

    assert used_filters == {
        'field1': ['*'],
        'field2!': ['*'],
    }
    assert query == {
        'query': {
            'bool': {
                'must': [],
                'must_not': [],
                'filter': [
                    {
                        'exists': {
                            'field': 'embedded.field1',
                        },
                    },
                    {
                        'missing': {
                            'field': 'embedded.field2',
                        }
                    }
                ],
            },
        },
    }
    assert result == {
        'filters': [
            {
                'field': 'field1',
                'term': '*',
                'remove': '/search/?field2%21=%2A',
            },
            {
                'field': 'field2!',
                'term': '*',
                'remove': '/search/?field1=%2A',
            }
        ],
    }


def test_set_facets():
    from collections import OrderedDict
    from snowflakes.search import set_facets
    facets = [
        ('type', {'title': 'Type'}),
        ('audit.foo', {'title': 'Audit'}),
        ('facet1', {'title': 'Facet 1'}),
    ]
    used_filters = OrderedDict((
        ('facet1', ['value1']),
        ('audit.foo', ['value2']),
    ))
    principals = ['group.admin']
    doc_types = ['Snowball']
    aggs = set_facets(facets, used_filters, principals, doc_types)

    expected = {
        'type': {
            'aggs': {
                'type': {
                    'terms': {
                        'field': 'embedded.@type',
                        'exclude': ['Item'],
                        'min_doc_count': 0,
                        'size': 100,
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                        {'terms': {'principals_allowed.view': ['group.admin']}},
                        {'terms': {'embedded.@type': ['Snowball']}},
                        {'terms': {'embedded.facet1': ['value1']}},
                        {'terms': {'audit.foo': ['value2']}},
                    ],
                },
            },
        },
        'audit-foo': {
            'aggs': {
                'audit-foo': {
                    'terms': {
                        'field': 'audit.foo',
                        'min_doc_count': 0,
                        'size': 100,
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                        {'terms': {'principals_allowed.view': ['group.admin']}},
                        {'terms': {'embedded.@type': ['Snowball']}},
                        {'terms': {'embedded.facet1': ['value1']}},
                    ],
                },
            },
        },
        'facet1': {
            'aggs': {
                'facet1': {
                    'terms': {
                        'field': 'embedded.facet1',
                        'min_doc_count': 0,
                        'size': 100,
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                        {'terms': {'principals_allowed.view': ['group.admin']}},
                        {'terms': {'embedded.@type': ['Snowball']}},
                        {'terms': {'audit.foo': ['value2']}},
                    ],
                },
            },
        }
    }
    assert(expected == aggs)


def test_set_facets_negated_filter():
    from collections import OrderedDict
    from snowflakes.search import set_facets
    facets = [
        ('facet1', {'title': 'Facet 1'}),
    ]
    used_filters = OrderedDict((
        ('field2!', ['value1']),
    ))
    principals = ['group.admin']
    doc_types = ['Snowball']
    aggs = set_facets(facets, used_filters, principals, doc_types)

    expected = {
        'facet1': {
            'aggs': {
                'facet1': {
                    'terms': {
                        'field': 'embedded.facet1',
                        'min_doc_count': 0,
                        'size': 100,
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                            {'terms': {'principals_allowed.view': ['group.admin']}},
                            {'terms': {'embedded.@type': ['Snowball']}},
                            {'not':
                                {'terms': {'embedded.field2': ['value1']}},
                            },
                        ],
                },
            },
        }
    }
    assert expected == aggs


def test_set_facets_type_exists():
    from collections import OrderedDict
    from snowflakes.search import set_facets
    facets = [
        ('field1', {'title': 'Facet 1', 'type': 'exists'}),
        ('field2', {'title': 'Facet 2', 'type': 'exists'}),
    ]
    used_filters = OrderedDict((
        ('field1', ['*']),
        ('field2!', ['*']),
    ))
    principals = ['group.admin']
    doc_types = ['Snowball']
    aggs = set_facets(facets, used_filters, principals, doc_types)

    expected = {
        'field1': {
            'aggs': {
                'field1': {
                    'filters': {
                        'filters': {
                            'yes': {
                                'exists': {'field': 'embedded.field1'}
                            },
                            'no': {
                                'missing': {'field': 'embedded.field1'}
                            }
                        },
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                        {'terms': {'principals_allowed.view': ['group.admin']}},
                        {'terms': {'embedded.@type': ['Snowball']}},
                        {'missing': {'field': 'embedded.field2'}},
                    ],
                },
            },
        },
        'field2': {
            'aggs': {
                'field2': {
                    'filters': {
                        'filters': {
                            'yes': {
                                'exists': {'field': 'embedded.field2'}
                            },
                            'no': {
                                'missing': {'field': 'embedded.field2'}
                            }
                        },
                    },
                },
            },
            'filter': {
                'bool': {
                    'must': [
                        {'terms': {'principals_allowed.view': ['group.admin']}},
                        {'terms': {'embedded.@type': ['Snowball']}},
                        {'exists': {'field': 'embedded.field1'}},
                    ],
                },
            },
        },
    }
    assert expected == aggs


def test_format_facets():
    from snowflakes.search import format_facets
    es_result = {
        'aggregations': {
            'field1': {
                'field1': {
                    'buckets': [
                        {
                            'key': 'value1',
                            'doc_count': 2,
                        },
                        {
                            'key': 'value2',
                            'doc_count': 1,
                        }
                    ]
                },
                'doc_count': 3,
            }
        }
    }
    facets = [
        ('field1', {'title': 'Field 1'}),
    ]
    used_filters = {}
    schemas = []
    total = 42
    principals = []
    result = format_facets(
        es_result, facets, used_filters, schemas, total, principals)

    assert result == [{
        'field': 'field1',
        'title': 'Field 1',
        'terms': [
            {
                'key': 'value1',
                'doc_count': 2,
            },
            {
                'key': 'value2',
                'doc_count': 1,
            }
        ],
        'total': 3,
        'type': 'terms',
    }]


def test_format_facets_no_aggregations():
    from snowflakes.search import format_facets
    result = format_facets({}, [], [], [], 0, [])
    assert result == []


def test_format_facets_skips_single_bucket_facets():
    from snowflakes.search import format_facets
    es_result = {
        'aggregations': {
            'field1': {
                'field1': {
                    'buckets': [
                        {
                            'key': 'value1',
                            'doc_count': 2,
                        },
                    ]
                },
                'doc_count': 2,
            }
        }
    }
    facets = [
        ('field1', {'title': 'Field 1'}),
    ]
    used_filters = {}
    schemas = []
    total = 42
    principals = []
    result = format_facets(
        es_result, facets, used_filters, schemas, total, principals)

    assert result == []


def test_format_facets_adds_pseudo_facet_for_extra_filters():
    from snowflakes.search import format_facets
    es_result = {
        'aggregations': {},
    }
    facets = []
    used_filters = {
        'title': ['titlevalue'],
    }
    schemas = [{
        'properties': {
            'title': {
                'title': 'Title',
            },
        },
    }]
    total = 42
    principals = []
    result = format_facets(
        es_result, facets, used_filters, schemas, total, principals)

    assert result == [{
        'field': 'title',
        'title': 'Title',
        'terms': [
            {
                'key': 'titlevalue',
            },
        ],
        'total': 42,
    }]
