import pytest


@pytest.fixture()
def snowflakes_facets():
    return {
        k: v
        for k, v in [
                ('type', {'title': 'Data Type', 'exclude': ['Item']}),
                ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
                ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
                ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
                ('status', {'title': 'Snowflake status'}),
                ('type', {'title': 'Snowflake type'}),
                ('lab.title', {'title': 'Lab'})
        ]
    }


@pytest.fixture
def raw_snowflakes_query():
    return {
        '_source': ['embedded.*'],
        'aggs': {
            'Snowflake type': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'type': {
                        'terms': {
                            'field': 'embedded.@type',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            },
            'Audit category: WARNING': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'audit-WARNING-category': {
                        'terms': {
                            'field': 'audit.WARNING.category',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            },
            'Data Type': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'type': {
                        'terms': {
                            'field': 'embedded.@type',
                            'size': 200,
                            'exclude': ['Item']
                        }
                    }
                }
            },
            'Audit category: ERROR': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'audit-ERROR-category': {
                        'terms': {
                            'field': 'audit.ERROR.category',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            },
            'Audit category: NOT COMPLIANT': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}]
                    }
                },
                'aggs': {
                    'audit-NOT_COMPLIANT-category': {
                        'terms': {
                            'field': 'audit.NOT_COMPLIANT.category',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            },
            'Snowflake status': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'status': {
                        'terms': {
                            'field': 'embedded.status',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            },
            'Lab': {
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['Snowflake']}}
                        ]
                    }
                },
                'aggs': {
                    'lab-title': {
                        'terms': {
                            'field': 'embedded.lab.title',
                            'size': 200,
                            'exclude': []
                        }
                    }
                }
            }
        },
        'query': {
            'bool': {
                'must': [
                    {
                        'terms': {
                            'principals_allowed.view': [
                                'system.Authenticated',
                                'group.admin',
                                'system.Everyone',
                                'remoteuser.TEST'
                            ]
                        }
                    },
                    {
                        'terms': {
                            'embedded.@type': ['Snowflake']}
                    }
                ]
            }
        },
        'post_filter': {
            'bool': {
                'must': [
                    {'terms': {'embedded.@type': ['Snowflake']}}]
            }
        }
    }


@pytest.fixture
def raw_snovault_query():
    return {
        'query': {
            'bool': {
                'must': [
                    {'terms': {'principals_allowed.view': ['system.Everyone']}},
                    {'terms': {'embedded.@type': ['TestingSearchSchema']}}
                ]
            }
        },
        'aggs': {
            'Status': {
                'aggs': {
                    'status': {
                        'terms': {
                            'size': 200,
                            'exclude': [],
                            'field': 'embedded.status'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            },
            'Audit category: ERROR': {
                'aggs': {
                    'audit-ERROR-category': {
                        'terms': {
                            'size': 200,
                            'exclude': [],
                            'field': 'audit.ERROR.category'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.status': ['released', 'archived']}},
                            {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}
                        ],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            },
            'Audit category: NOT COMPLIANT': {
                'aggs': {
                    'audit-NOT_COMPLIANT-category': {
                        'terms': {
                            'size': 200,
                            'exclude': [],
                            'field': 'audit.NOT_COMPLIANT.category'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.status': ['released', 'archived']}},
                            {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            },
            'Data Type': {
                'aggs': {
                    'type': {
                        'terms': {
                            'size': 200,
                            'exclude': ['Item'],
                            'field': 'embedded.@type'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.status': ['released', 'archived']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}
                        ],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            },
            'Name': {
                'aggs': {
                    'name': {
                        'terms': {
                            'size': 200,
                            'exclude': [],
                            'field': 'embedded.name'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.status': ['released', 'archived']}},
                            {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}
                        ],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            },
            'Audit category: WARNING': {
                'aggs': {
                    'audit-WARNING-category': {
                        'terms': {
                            'size': 200,
                            'exclude': [],
                            'field': 'audit.WARNING.category'
                        }
                    }
                },
                'filter': {
                    'bool': {
                        'must': [
                            {'terms': {'embedded.status': ['released', 'archived']}},
                            {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                            {'terms': {'embedded.file_format': ['bam']}},
                            {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                            {'exists': {'field': 'embedded.dbxref'}}
                        ],
                        'must_not': [
                            {'terms': {'embedded.lab.name': ['thermo']}},
                            {'exists': {'field': 'embedded.restricted'}}
                        ]
                    }
                }
            }
        },
        '_source': ['embedded.*'],
        'post_filter': {
            'bool': {
                'must': [
                    {'terms': {'embedded.status': ['released', 'archived']}},
                    {'terms': {'embedded.@type': ['TestingSearchSchema']}},
                    {'terms': {'embedded.file_format': ['bam']}},
                    {'terms': {'embedded.replcate.biosample.title': ['cell']}},
                    {'exists': {'field': 'embedded.dbxref'}}
                ],
                'must_not': [
                    {'terms': {'embedded.lab.name': ['thermo']}},
                    {'exists': {'field': 'embedded.restricted'}}
                ]
            }
        }
    }


@pytest.fixture
def raw_response():
    return {
        'took': 43,
        'timed_out': False,
        'hits': {
            'hits': [
                {
                    '_id': '582d6675-0b2e-4886-b43d-f6efa9033b37',
                    '_index': 'snowflake',
                    '_source': {
                        'embedded': {
                            'date_created': '2013-03-23',
                            'schema_version': '1',
                            'snowset': '/snowballs/SNOSS000ACY/',
                            'uuid': '582d6675-0b2e-4886-b43d-f6efa9033b37',
                            'accession': 'SNOFL000LSQ',
                            '@id': '/snowflakes/SNOFL000LSQ/',
                            'type': 'fluffy',
                            'award': '/awards/U41HG006992/',
                            'status': 'deleted',
                            'lab': {
                                'status': 'current',
                                'phone1': '000-000-0000',
                                'name': 'j-michael-cherry',
                                'schema_version': '3',
                                '@id': '/labs/j-michael-cherry/',
                                'uuid': 'cfb789b8-46f3-4d59-a2b3-adc39e7df93a',
                                'institute_name': 'Stanford University',
                                'institute_label': 'Stanford',
                                'awards': [
                                    '/awards/U41HG006992/'
                                ],
                                'city': 'Stanford',
                                '@type': ['Lab', 'Item'],
                                'title': 'J. Michael Cherry, Stanford',
                                'postal_code': '94304-5577',
                                'fax': '000-000-0000',
                                'phone2': '000-000-0000',
                                'address2': '300 Pasteur Drive; MC5120',
                                'state': 'CA',
                                'address1': 'Department of Genetics',
                                'pi': '/users/860c4750-8d3c-40f5-8f2c-90c5e5d19e88/',
                                'country': 'USA'
                            },
                            '@type': ['Snowflake', 'Item'],
                            'submitted_by': {
                                'lab': '/labs/w-james-kent/',
                                'title': 'W. James Kent',
                                '@type': ['User', 'Item'],
                                'uuid': '746eef27-d857-4b38-a469-cac93fb02164',
                                '@id': '/users/746eef27-d857-4b38-a469-cac93fb02164/'
                            }
                        }
                    },
                    '_type': 'snowflake',
                    '_score': 1.5281246
                },
                {
                    '_id': 'f76ae1b9-6bb4-4cc9-8dcc-2bb5cea877e1',
                    '_index': 'snowflake',
                    '_source': {
                        'embedded': {
                            'date_created': '2014-01-21',
                            'schema_version': '1',
                            'snowset': '/snowballs/SNOSS000ACT/',
                            'uuid': 'f76ae1b9-6bb4-4cc9-8dcc-2bb5cea877e1',
                            'accession': 'SNOFL001RID',
                            '@id': '/snowflakes/SNOFL001RID/',
                            'type': 'wet',
                            'award': '/awards/U41HG006992/',
                            'status': 'in progress',
                            'lab': {
                                'status': 'current',
                                'phone1': '000-000-0000',
                                'name': 'j-michael-cherry',
                                'schema_version': '3',
                                '@id': '/labs/j-michael-cherry/',
                                'uuid': 'cfb789b8-46f3-4d59-a2b3-adc39e7df93a',
                                'institute_name': 'Stanford University',
                                'institute_label': 'Stanford',
                                'awards': ['/awards/U41HG006992/'],
                                'city':
                                'Stanford',
                                '@type': ['Lab', 'Item'],
                                'title': 'J. Michael Cherry, Stanford',
                                'postal_code': '94304-5577',
                                'fax': '000-000-0000',
                                'phone2': '000-000-0000',
                                'address2': '300 Pasteur Drive; MC5120',
                                'state': 'CA',
                                'address1': 'Department of Genetics',
                                'pi': '/users/860c4750-8d3c-40f5-8f2c-90c5e5d19e88/',
                                'country': 'USA'
                            },
                            '@type': ['Snowflake', 'Item'],
                            'submitted_by': {
                                'lab': '/labs/richard-myers/',
                                'title': 'Tincidunt Volutpat-Ullamcorper',
                                '@type': ['User', 'Item'],
                                'uuid': 'df9f3c8e-b819-4885-8f16-08f6ef0001e8',
                                '@id': '/users/df9f3c8e-b819-4885-8f16-08f6ef0001e8/'
                            }
                        }
                    },
                    '_type': 'snowflake',
                    '_score': 1.5281246
                }
            ],
            '_shards': {
                    'skipped': 0,
                    'total': 85,
                    'successful': 85,
                    'failed': 0
            },
            'aggregations': {
                'Lab': {
                    'lab-title': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': [
                            {'key': 'J. Michael Cherry, Stanford', 'doc_count': 35}
                        ]
                    },
                    'doc_count': 35
                },
                'Snowflake status': {
                    'status': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': [
                            {'key': 'released', 'doc_count': 21},
                            {'key': 'in progress', 'doc_count': 11},
                            {'key': 'revoked', 'doc_count': 2},
                            {'key': 'deleted', 'doc_count': 1}
                        ]
                    },
                    'doc_count': 35
                },
                'Audit category: ERROR': {
                    'audit-ERROR-category': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': []
                    },
                    'doc_count': 35
                },
                'Data Type': {
                    'type': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': [
                            {'key': 'Snowflake', 'doc_count': 35}
                        ]
                    },
                    'doc_count': 35
                },
                'Snowflake type': {
                    'type': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': [
                            {'key': 'Item', 'doc_count': 35},
                            {'key': 'Snowflake', 'doc_count': 35}
                        ]
                    },
                    'doc_count': 35
                },
                'Audit category: NOT COMPLIANT': {
                    'doc_count': 35,
                    'audit-NOT_COMPLIANT-category': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': []
                    }
                },
                'Audit category: WARNING': {
                    'doc_count': 35,
                    'audit-WARNING-category': {
                        'sum_other_doc_count': 0,
                        'doc_count_error_upper_bound': 0,
                        'buckets': []
                    }
                },
                'is_restricted': {
                    'buckets': {
                        'no': {
                            'doc_count': 19908
                        },
                        'yes': {
                            'doc_count': 1053
                        }
                    }
                }
            }
        }
    }


@pytest.fixture
def parsed_params(dummy_request):
    from pyramid.testing import DummyResource
    from pyramid.security import Allow
    from snovault.elasticsearch.searches.parsers import ParamsParser
    dummy_request.environ['REMOTE_USER'] = 'TEST_SUBMITTER'
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
        '&biosample_ontology.term_name%21=naive+thymus-derived+CD4-positive%2C+alpha-beta+T+cell'
        '&file_type=bam&file_type=bigwig&file_type!=fastq&biosample.genetic_modifications=*'
        '&biosample.treatments!=ethanol&biosample.treatments!=methanol&restricted!=*'
        '&limit=10&status=released&searchTerm=chip-seq&sort=date_created&sort=-files.file_size'
        '&field=@id&field=accession'
    )
    dummy_request.context = DummyResource()
    dummy_request.context.__acl__ = lambda: [(Allow, 'group.submitter', 'search_audit')]
    params_parser = ParamsParser(dummy_request)
    return params_parser


@pytest.fixture
def basic_search_query_factory_with_facets(raw_snowflakes_query, parsed_params):
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    from elasticsearch_dsl import Search
    bsq = BasicSearchQueryFactoryWithFacets(parsed_params)
    bsq.search = Search().from_dict(raw_snowflakes_query)
    return bsq


@pytest.fixture
def basic_query_response_with_facets(raw_response, basic_search_query_factory_with_facets):
    from snovault.elasticsearch.searches.responses import BasicQueryResponseWithFacets
    from elasticsearch_dsl.response import Response, AggResponse
    basic_search_query_factory_with_facets.search._response = Response(
        basic_search_query_factory_with_facets.search,
        raw_response
    )
    ar = AggResponse(
        basic_search_query_factory_with_facets.search.aggs,
        basic_search_query_factory_with_facets.search,
        raw_response['hits']['aggregations']
    )
    basic_search_query_factory_with_facets.search._response._aggs = ar
    bqr = BasicQueryResponseWithFacets(
        results=basic_search_query_factory_with_facets.search._response,
        query_builder=basic_search_query_factory_with_facets
    )
    return bqr


def test_searches_mixins_aggs_to_facets_mixin_get_total(basic_query_response_with_facets):
    assert basic_query_response_with_facets._get_total() == 2


def test_searches_mixins_aggs_to_facets_mixin_get_aggregations(
        basic_query_response_with_facets,
        raw_response,
        snowflakes_facets
):
    expected = raw_response['hits']['aggregations']
    actual = basic_query_response_with_facets._get_aggregations()
    for k in expected:
        assert k in actual
    for k, v in snowflakes_facets.items():
        assert (
            len(expected[v.get('title')][basic_query_response_with_facets._get_facet_name(k)]['buckets'])
            == len(actual[v.get('title')][basic_query_response_with_facets._get_facet_name(k)]['buckets'])
        )


def test_searches_mixins_aggs_to_facets_mixin_get_facets(basic_query_response_with_facets):
    expected = {
        'type': {
            'exclude': ['Item'],
            'title': 'Data Type'
        },
        'audit.INTERNAL_ACTION.category': {
            'title': 'Audit category: DCC ACTION'
        },
        'audit.NOT_COMPLIANT.category': {
            'title': 'Audit category: NOT COMPLIANT'
        },
        'name': {
            'title': 'Name'
        },
        'audit.ERROR.category': {
            'title': 'Audit category: ERROR'
        },
        'audit.WARNING.category': {
            'title': 'Audit category: WARNING'
        },
        'status': {
            'title': 'Status'
        }
    }
    assert basic_query_response_with_facets._get_facets() == expected


def test_searches_mixins_aggs_to_facets_mixin_get_facets_called_once(mocker, parsed_params):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    afm = AggsToFacetsMixin()
    bsq = BasicSearchQueryFactoryWithFacets(parsed_params)
    afm.query_builder = bsq
    mocker.patch.object(BasicSearchQueryFactoryWithFacets, '_get_facets')
    facets = {'test': 1}
    BasicSearchQueryFactoryWithFacets._get_facets.return_value = facets.items()
    f = afm._get_facets()
    assert f == facets
    assert BasicSearchQueryFactoryWithFacets._get_facets.call_count == 1
    f = afm._get_facets()
    assert f == facets
    # Test lru_cache.
    assert BasicSearchQueryFactoryWithFacets._get_facets.call_count == 1


def test_searches_mixins_aggs_to_facets_mixin_get_facet_name():
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    afm = AggsToFacetsMixin()
    assert afm._get_facet_name('target') == 'target'
    assert afm._get_facet_name('experiment.library') == 'experiment-library'
    assert afm._get_facet_name('experiment.library.biosample') == 'experiment-library-biosample'


def test_searches_mixins_aggs_to_facets_mixin_get_facet_title(basic_query_response_with_facets):
    assert basic_query_response_with_facets._get_facet_title('type') == 'Data Type'
    assert basic_query_response_with_facets._get_facet_title('status') == 'Status'
    assert basic_query_response_with_facets._get_facet_title(
        'audit.WARNING.category'
    ) == 'Audit category: WARNING'


def test_searches_mixins_aggs_to_facets_mixin_get_facet_type(basic_query_response_with_facets, mocker):
    assert basic_query_response_with_facets._get_facet_type('status') == 'terms'
    from snovault.elasticsearch.searches.responses import BasicQueryResponseWithFacets
    mocker.patch.object(BasicQueryResponseWithFacets, '_get_facets')
    BasicQueryResponseWithFacets._get_facets.return_value = {
        'status': {
            'title': 'Status',
            'type': 'exists'
        }
    }
    assert basic_query_response_with_facets._get_facet_type('status') == 'exists'


def test_searches_mixins_aggs_to_facets_mixin_parse_aggregation_bucket_to_list(raw_response):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    afm = AggsToFacetsMixin()
    expected = [{'doc_count': 1053, 'key': 'yes'}, {'key': 'no', 'doc_count': 19908}]
    actual = afm._parse_aggregation_bucket_to_list(
        raw_response['hits']['aggregations']['is_restricted']['buckets']
    )
    assert all([e in actual for e in expected])
    assert len(actual) == len(expected)


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_result(
        basic_query_response_with_facets,
        mocker,
        snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    expected = {
        'doc_count': 35,
        'status': {
            'buckets': [
                {'doc_count': 21, 'key': 'released'},
                {'doc_count': 11, 'key': 'in progress'},
                {'doc_count': 2, 'key': 'revoked'},
                {'doc_count': 1, 'key': 'deleted'}
            ],
            'doc_count_error_upper_bound': 0,
            'sum_other_doc_count': 0
        }
    }
    actual = basic_query_response_with_facets._get_aggregation_result('status')
    assert all([e in actual['status']['buckets'] for e in expected['status']['buckets']])
    assert len(expected['status']['buckets']) == len(actual['status']['buckets'])


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_bucket(
        basic_query_response_with_facets,
        mocker,
        snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    expected = [
        {'doc_count': 21, 'key': 'released'},
        {'doc_count': 11, 'key': 'in progress'},
        {'doc_count': 2, 'key': 'revoked'},
        {'doc_count': 1, 'key': 'deleted'}
    ]
    actual = basic_query_response_with_facets._get_aggregation_bucket('status')
    assert all([e in actual for e in expected])
    assert len(expected) == len(actual)
    expected = [
        {'key': 'J. Michael Cherry, Stanford', 'doc_count': 35}
    ]
    actual = basic_query_response_with_facets._get_aggregation_bucket('lab.title')
    assert all([e in actual for e in expected])
    assert len(expected) == len(actual)


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_total(
        basic_query_response_with_facets,
        mocker,
        snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    assert basic_query_response_with_facets._get_aggregation_total('status') == 35
    assert basic_query_response_with_facets._get_aggregation_total('lab.title') == 35
    assert basic_query_response_with_facets._get_aggregation_total('type') == 35


def test_searches_mixins_aggs_to_facets_mixin_aggregation_is_appended(
        basic_query_response_with_facets,
        mocker,
        snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    assert not basic_query_response_with_facets._aggregation_is_appended('status')
    assert not basic_query_response_with_facets._aggregation_is_appended('lab.title')
    assert basic_query_response_with_facets._aggregation_is_appended('new_filter')


def test_searches_mixins_aggs_to_facets_mixin_format_aggregation(
        basic_query_response_with_facets,
        mocker,
        snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    basic_query_response_with_facets._format_aggregation('status')
    expected = [
        {
            'terms': [
                {'key': 'released', 'doc_count': 21},
                {'key': 'in progress', 'doc_count': 11},
                {'key': 'revoked', 'doc_count': 2},
                {'key': 'deleted', 'doc_count': 1}
            ],
            'total': 35,
            'field': 'status',
            'appended': 'false',
            'type': 'terms',
            'title': 'Snowflake status'
        }
    ]
    actual = basic_query_response_with_facets.facets
    assert all([e in actual[0]['terms'] for e in expected[0]['terms']])
    assert expected[0]['total'] == actual[0]['total']
    assert expected[0]['field'] == actual[0]['field']
    assert expected[0]['title'] == actual[0]['title']
    assert expected[0]['type'] == actual[0]['type']
    assert expected[0]['appended'] == actual[0]['appended']


def test_searches_mixins_aggs_to_facets_mixin_format_aggregations(
    basic_query_response_with_facets,
    mocker,
    snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    basic_query_response_with_facets._format_aggregations()
    expected = [
        {
            'field': 'status',
            'terms': [
                {'doc_count': 21, 'key': 'released'},
                {'doc_count': 11, 'key': 'in progress'},
                {'doc_count': 2, 'key': 'revoked'},
                {'doc_count': 1, 'key': 'deleted'}
            ],
            'appended': 'false',
            'title': 'Snowflake status',
            'total': 35,
            'type': 'terms'
        },
        {
            'field': 'lab.title',
            'terms': [
                {'doc_count': 35, 'key': 'J. Michael Cherry, Stanford'}
            ],
            'appended': 'false',
            'title': 'Lab',
            'total': 35,
            'type': 'terms'
        },
        {
            'field': 'type',
            'terms': [
                {'doc_count': 35, 'key': 'Item'},
                {'doc_count': 35, 'key': 'Snowflake'}
            ],
            'appended': 'false',
            'title': 'Snowflake type',
            'total': 35,
            'type': 'terms'
        }
    ]
    actual = basic_query_response_with_facets.facets
    assert len(expected) == len(actual)
    for e in expected:
        a = [a for a in actual if e['title'] == a['title']][0]
        assert all([e in a['terms'] for e in a['terms']])
        assert e['total'] == a['total']
        assert e['field'] == a['field']
        assert e['title'] == a['title']
        assert e['type'] == a['type']
        assert e['appended'] == a['appended']


def test_searches_mixins_aggs_to_facets_mixin_get_fake_facets(
    basic_query_response_with_facets,
    mocker,
    snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    expected = [
        ('assay_title', 'Histone ChIP-seq'),
        ('award.project', 'Roadmap'),
        ('assembly', 'GRCh38'),
        ('biosample_ontology.classification', 'primary cell'),
        ('target.label', 'H3K27me3'),
        ('biosample_ontology.classification!', 'cell line'),
        ('biosample_ontology.term_name!', 'naive thymus-derived CD4-positive, alpha-beta T cell'),
        ('file_type', 'bam'),
        ('file_type', 'bigwig'),
        ('file_type!', 'fastq'),
        ('biosample.genetic_modifications', '*'),
        ('biosample.treatments!', 'ethanol'),
        ('biosample.treatments!', 'methanol'),
        ('restricted!', '*')
    ]
    actual = basic_query_response_with_facets._get_fake_facets()
    assert all([e in actual for e in expected])
    assert len(actual) == len(expected)


def test_searches_mixins_aggs_to_facets_mixin_make_fake_bucket():
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    am = AggsToFacetsMixin()
    am._make_fake_bucket('target.name', 'ctcf', 'false')
    assert am.fake_buckets['target.name'][0] == {'key': 'ctcf', 'isEqual': 'false'}


def test_searches_mixins_aggs_to_facets_mixin_make_fake_buckets():
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    am = AggsToFacetsMixin()
    am._make_fake_buckets(
        params=[('status', 'released'), ('treated', '*'), ('biosample.organism', 'human')],
        is_equal='true'
    )
    actual = dict(am.fake_buckets)
    expected = {
        'biosample.organism': [
            {'key': 'human', 'isEqual': 'true'}
        ],
        'status': [
            {'key': 'released', 'isEqual': 'true'}
        ],
        'treated': [
            {'key': '*', 'isEqual': 'true'}
        ]
    }
    assert actual == expected


def test_searches_mixins_aggs_to_facets_mixin_make_fake_buckets_from_fake_facet(
        basic_query_response_with_facets
):
    basic_query_response_with_facets._make_fake_buckets_from_fake_facets(
        [
            ('status', 'released'),
            ('status', 'in progress'),
            ('status!', 'archived'),
            ('file_format', '*'),
            ('restricted!', '*'),
        ]
    )
    expected = {
        'file_format': [
            {'key': '*', 'isEqual': 'true'}
        ],
        'status': [
            {'key': 'released', 'isEqual': 'true'},
            {'key': 'in progress', 'isEqual': 'true'},
            {'key': 'archived', 'isEqual': 'false'}
        ],
        'restricted': [
            {'key': '*', 'isEqual': 'false'}
        ]
    }
    actual = dict(basic_query_response_with_facets.fake_buckets)
    for x in expected.keys():
        assert all([te in actual.get(x) for te in expected.get(x)])
        assert len(actual.get(x)) == len(expected.get(x))


def test_searches_mixins_aggs_to_facets_mixin_make_fake_facet(basic_query_response_with_facets):
    basic_query_response_with_facets._make_fake_facet('target.name', [{'key': '*', 'isEqual': 'true'}])
    assert basic_query_response_with_facets.fake_facets[0] == {
        'field': 'target.name',
        'appended': 'true',
        'title': 'target.name',
        'total': 2,
        'terms': [
            {'key': '*', 'isEqual': 'true'}
        ]
    }


def test_searches_mixins_aggs_to_facets_mixin_make_fake_facets(
    basic_query_response_with_facets,
    mocker,
    snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    basic_query_response_with_facets._make_fake_facets()
    assert len(basic_query_response_with_facets.fake_facets) == 10


def test_searches_mixins_aggs_to_facets_mixin_to_facets(
    basic_query_response_with_facets,
    mocker,
    snowflakes_facets
):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    mocker.patch.object(AggsToFacetsMixin, '_get_facets')
    AggsToFacetsMixin._get_facets.return_value = snowflakes_facets
    actual = basic_query_response_with_facets.to_facets()
    assert len(actual) == 13


def test_searches_mixins_hits_to_graph_mixin_get_results(
        basic_query_response_with_facets,
        raw_response,
        mocker
):
    from elasticsearch_dsl.response import Response
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    res = basic_query_response_with_facets._get_results()
    assert isinstance(res, Response)
    mocker.patch.object(BasicSearchQueryFactoryWithFacets, '_should_scan_over_results')
    BasicSearchQueryFactoryWithFacets._should_scan_over_results.return_value = True
    from types import GeneratorType
    res = basic_query_response_with_facets._get_results()
    assert isinstance(res, GeneratorType)


def test_searches_mixins_hits_to_graph_mixin_to_graph(
        basic_query_response_with_facets,
        raw_response
):
    r = basic_query_response_with_facets.to_graph()
    assert len(r) == len(raw_response['hits']['hits'])
    assert all(['accession' in x for x in r])



