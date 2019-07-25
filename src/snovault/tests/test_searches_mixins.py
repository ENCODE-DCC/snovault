import pytest


@pytest.fixture
def raw_query():
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
                }
            }
        }
    }


@pytest.fixture
def basic_search_query_factory_with_facets(raw_query, dummy_request):
    from pyramid.testing import DummyResource
    from pyramid.security import Allow
    from snovault.elasticsearch.searches.parsers import ParamsParser
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    from elasticsearch_dsl import Search
    dummy_request.environ['REMOTE_USER'] = 'TEST_SUBMITTER'
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
        '&biosample_ontology.term_name%21=naive+thymus-derived+CD4-positive%2C+alpha-beta+T+cell'
        '&limit=10&status=released&searchTerm=chip-seq&sort=date_created&sort=-files.file_size'
        '&field=@id&field=accession'
    )
    dummy_request.context = DummyResource()
    dummy_request.context.__acl__ = lambda: [(Allow, 'group.submitter', 'search_audit')]
    params_parser = ParamsParser(dummy_request)
    bsq = BasicSearchQueryFactoryWithFacets(params_parser)
    bsq.search = Search().from_dict(raw_query)
    return bsq


@pytest.fixture
def basic_query_response_with_facets(raw_response, basic_search_query_factory_with_facets):
    from snovault.elasticsearch.searches.responses import BasicQueryResponseWithFacets
    bqr = BasicQueryResponseWithFacets(
        results=raw_response,
        query_builder=basic_search_query_factory_with_facets
    )
    return bqr


def test_searches_mixins_aggs_to_facets_mixin_get_aggregations(basic_query_response_with_facets):
    assert False


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


def test_searches_mixins_aggs_to_facets_mixin_get_facets_called_once(mocker, dummy_request):
    from snovault.elasticsearch.searches.mixins import AggsToFacetsMixin
    from snovault.elasticsearch.searches.queries import BasicSearchQueryFactoryWithFacets
    from pyramid.testing import DummyResource
    from pyramid.security import Allow
    from snovault.elasticsearch.searches.parsers import ParamsParser
    dummy_request.environ['REMOTE_USER'] = 'TEST_SUBMITTER'
    dummy_request.environ['QUERY_STRING'] = (
        'type=TestingSearchSchema&assay_title=Histone+ChIP-seq&award.project=Roadmap'
        '&assembly=GRCh38&biosample_ontology.classification=primary+cell'
        '&target.label=H3K27me3&biosample_ontology.classification%21=cell+line'
        '&biosample_ontology.term_name%21=naive+thymus-derived+CD4-positive%2C+alpha-beta+T+cell'
        '&limit=10&status=released&searchTerm=chip-seq&sort=date_created&sort=-files.file_size'
        '&field=@id&field=accession'
    )
    dummy_request.context = DummyResource()
    dummy_request.context.__acl__ = lambda: [(Allow, 'group.submitter', 'search_audit')]
    params_parser = ParamsParser(dummy_request)
    afm = AggsToFacetsMixin()
    bsq = BasicSearchQueryFactoryWithFacets(params_parser)
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
    assert False


def test_searches_mixins_aggs_to_facets_mixin_get_facet_title():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_get_facet_type():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_parse_aggregation_bucket_to_list():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_result():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_bucket():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_get_aggregation_total():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_aggregation_is_appeneded():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_format_aggregation():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_format_aggregations():
    assert False


def test_searches_mixins_aggs_to_facets_mixin_to_facets():
    assert False


def test_searches_mixins_hits_to_graph_mixin_to_graph():
    assert False
