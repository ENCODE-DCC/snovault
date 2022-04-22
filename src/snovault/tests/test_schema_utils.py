from snovault.schema_utils import validate
import pytest


targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
]


@pytest.fixture
def content(testapp):
    url = '/testing-link-targets/'
    for item in targets:
        testapp.post_json(url, item, status=201)


def test_uniqueItems_validates_normalized_links(content, threadlocals):
    schema = {
        'properties': {
            'some_links': {
                'uniqueItems': True,
                'items': {
                    'linkTo': 'TestingLinkTarget',
                }
            }
        }
    }
    uuid = targets[0]['uuid']
    data = [
        uuid,
        '/testing-link-targets/{}'.format(uuid),
    ]
    validated, errors = validate(schema, {'some_links': data})
    assert len(errors) == 1
    assert (
        errors[0].message == "['{}', '{}'] has non-unique elements".format(
            uuid, uuid)
    )


def test_schema_utils_update_resolved_data(mocker):
    from snovault.schema_utils import _update_resolved_data
    from snovault.schema_utils import resolve_merge_ref
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    _update_resolved_data(resolved_data, 'xyz', {})
    assert resolved_data == {
        'a': 'new value',
        'and': 'a ref',
        'inside': 'of a ref',
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': [
                'lists'
            ]
        }
    }


def test_schema_utils_handle_list_or_string_value(mocker):
    from snovault.schema_utils import _handle_list_or_string_value
    from snovault.schema_utils import resolve_merge_ref
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = 'notxyz'
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': [
                'lists'
            ]
        }
    }
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'the': 'notxyz values',
        'with': {
            'sub': 'dependencies',
            'and': ['lists']
        },
        'a': 'new value',
        'and': 'a ref',
        'inside': 'of a ref'
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'b',
            }
        else:
            return {
                'c': 'd'
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'b',
        'c': 'd',
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'b',
            }
        else:
            return {
                'a': 'override'
            }
    resolve_merge_ref.side_effect = custom_resolver
    resolved_data = {}
    value = ['notxyz', 'xyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'b',
    }
    resolved_data = {}
    value = ['xyz', 'notxyz']
    _handle_list_or_string_value(resolved_data, value, {})
    assert resolved_data == {
        'a': 'override',
    }


def test_schema_utils_resolve_merge_refs_returns_copy_of_original_if_no_refs(mocker):
    from snovault.schema_utils import resolve_merge_refs
    resolver = None
    data = {'a': 'b'}
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    assert id(resolved_data) != id(data)
    data = {
        'a': 'b',
        'c': ['d', 'e', 1],
        'x': {
            'y': {
                'z': [
                    {
                        'p': 'r'
                    },
                    3.2,
                    True,
                    False,
                    None
                ]
            }
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    # Dicts are copies.
    assert id(resolved_data) != id(data)
    # Lists are copies.
    assert id(resolved_data['x']['y']['z'][0]) != id(data['x']['y']['z'][0])
    # Assignment doesn't reflect in original.
    resolved_data['x']['y']['z'][0]['p'] = 'new value'
    assert data['x']['y']['z'][0]['p'] == 'r'
    data = [
        'k',
        '5',
        '6',
        2,
        {},
    ]
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    assert id(resolved_data) != id(data)


def test_schema_utils_resolve_merge_refs_fills_in_refs(mocker):
    from snovault.schema_utils import resolve_merge_refs
    resolver = None
    data = {'a': 'b'}
    resolved_data = resolve_merge_refs(data, resolver)
    assert resolved_data == data
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    resolve_merge_ref.return_value = {'a new value': 'that was resolved'}
    data = {'a': 'b', 'c': {'$merge': 'xyz'}}
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {'a': 'b', 'c': {'a new value': 'that was resolved'}}
    assert resolved_data == expected_data
    data = {
        'a': 'b',
        'c': {'$merge': 'xyz'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        '$merge': 'xyz',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'a': 'b',
        'c': {'a new value': 'that was resolved'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        'a new value': 'that was resolved',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    def custom_resolver(ref, resolver):
        if ref == 'xyz':
            return {
                'a': 'new value',
                'and': 'a ref',
                'inside': 'of a ref',
                '$merge': 'notxyz',
            }
        else:
            return {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists'],
                }
            }
    resolve_merge_ref.side_effect = custom_resolver
    data = {
        'something_new': [
            {
                '$merge': 'notxyz'
            }
        ],
        'a': 'b',
        'c': {'$merge': 'xyz'},
        'sub': {
            'values': [
                'that',
                'were',
                'resolved',
                {
                    'if': {
                        '$merge': 'xyz',
                        'and': 'other',
                        'values': 'are',
                        'allowed': 'too',
                    }
                }
            ]
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'something_new': [
            {
                'the': 'notxyz values',
                'with': {
                    'sub': 'dependencies',
                    'and': ['lists']
                }
            }
        ],
        'a': 'b',
        'c': {
            'a': 'new value',
            'and': 'a ref',
            'inside': 'of a ref',
            'the': 'notxyz values',
            'with': {
                'sub': 'dependencies',
                'and': ['lists']
            }
        },
        'sub': {
            'values': [
                'that', 'were',
                'resolved', {
                    'if': {
                        'a': 'new value',
                        'and': 'other',
                        'inside': 'of a ref',
                        'the': 'notxyz values',
                        'with': {
                            'sub': 'dependencies',
                            'and': ['lists']
                        },
                        'values': 'are',
                        'allowed': 'too'
                    }
                }
            ]
        }
    }
    assert resolved_data == expected_data


def test_schema_utils_resolve_merge_refs_fills_allows_override_of_ref_property(mocker):
    from snovault.schema_utils import resolve_merge_refs
    resolver = None
    resolve_merge_ref = mocker.patch('snovault.schema_utils.resolve_merge_ref')
    resolve_merge_ref.return_value = {
        'a new value': 'that was resolved',
        'custom': 'original value',
        'and': 'something else',
    }
    data = {
        'a': {
            '$merge': 'xyz',
            'custom': 'override',
        }
    }
    resolved_data = resolve_merge_refs(data, resolver)
    expected_data = {
        'a': {
            'a new value': 'that was resolved',
            'and': 'something else',
            'custom': 'override',
        }
    }
    assert resolved_data == expected_data


def test_schema_utils_resolve_merge_ref_in_real_schema():
    import codecs
    import json
    from pyramid.path import AssetResolver
    from snovault.schema_utils import resolve_merge_ref
    from snovault.schema_utils import resolve_merge_refs
    from jsonschema import RefResolver
    filename = 'snowflakes:schemas/snowball.json'
    utf8 = codecs.getreader('utf-8')
    asset = AssetResolver(
        'snowflakes'
    ).resolve(
        filename
    )
    schema = json.load(
        utf8(
            asset.stream()
        ),
        object_pairs_hook=dict
    )
    assert list(schema['properties'].keys()) == [
        'schema_version',
        'method'
    ]
    resolver = RefResolver('file://' + asset.abspath(), schema)

    # Try resolving ref from mixins.
    resolved = resolve_merge_ref(
        'mixins.json#/uuid',
        resolver
    )
    assert resolved == {
        'uuid': {
            'title': 'UUID',
            'type': 'string',
            'format': 'uuid',
            'serverDefault': 'uuid4',
            'requestMethod': ['POST']
        }
    }
    # Resolve the inner object.
    resolved = resolve_merge_ref(
        'mixins.json#/uuid/uuid',
        resolver
    )
    assert resolved == {
        'title': 'UUID',
        'type': 'string',
        'format': 'uuid',
        'serverDefault': 'uuid4',
        'requestMethod': ['POST']
    }
    # Raise error if resolve value not dict.
    with pytest.raises(ValueError) as error:
        resolved = resolve_merge_ref(
            'mixins.json#/uuid/uuid/title',
            resolver
        )
    assert str(error.value) == (
        "Schema ref mixins.json#/uuid/uuid/title must resolve dict, not <class 'str'>"
    )
    # Add ref to properties.
    schema['properties']['accession'] = {
        '$merge': 'mixins.json#/accession/accession'
    }
    # Resolve from properties.
    resolved = resolve_merge_refs(schema['properties'], resolver)
    expected = {
        'schema_version': {
            'default': '2', 'comment': 'For testing upgrades'
        },
        'method': {
            'title': 'Method',
            'description': 'Technique used to make snowball.',
            'type': 'string',
            'default': 'hand-packed',
            'enum': [
                'hand-packed',
                'scoop-formed',
                'accreted'
            ]
        },
        'accession': {
            'title': 'Accession',
            'description': 'A unique identifier to be used to reference the object prefixed with a db specific char set',
            'comment': 'Do not submit. The accession is assigned by the server.',
            'type': 'string',
            'format': 'accession',
            'serverDefault': 'accession',
            'permission': 'import_items'
        }
    }
    assert resolved == expected
    # Fill in status from top level
    del schema['properties']['accession']
    schema['properties']['$merge'] = 'mixins.json#/standard_status'
    resolved = resolve_merge_refs(schema['properties'], resolver)
    expected = {
        'schema_version': {
            'default': '2',
            'comment': 'For testing upgrades'
        },
        'method': {
            'title': 'Method',
            'description': 'Technique used to make snowball.',
            'type': 'string',
            'default': 'hand-packed',
            'enum': [
                'hand-packed',
                'scoop-formed',
                'accreted'
            ]
        },
        'status': {
            'title': 'Status',
            'type': 'string',
            'default': 'in progress',
            'enum': [
                'in progress',
                'deleted',
                'replaced',
                'released'
            ]
        }
    }
    assert resolved == expected
    schema['properties'] = {}
    schema['properties']['$merge'] = [
        'mixins.json#/standard_status',
        'mixins.json#/attachment',
        'mixins.json#/submitted',
    ]
    resolved = resolve_merge_refs(schema['properties'], resolver)
    expected = {
        'status': {
            'title': 'Status',
            'type': 'string',
            'default': 'in progress',
            'enum': [
                'in progress',
                'deleted',
                'replaced',
                'released'
            ]
        },
        'attachment': {
            'title': 'Document file metadata',
            'type': 'object',
            'additionalProperties': False,
            'formInput': 'file',
            'attachment': True,
            'properties': {
                'download': {
                    'title': 'File Name',
                    'type': 'string'
                },
                'href': {
                    'comment': 'Internal webapp URL for document file',
                    'type': 'string'
                },
                'type': {
                    'title': 'MIME type',
                    'type': 'string',
                    'enum': [
                        'application/pdf',
                        'text/plain',
                        'text/tab-separated-values',
                        'image/jpeg',
                        'image/tiff',
                        'image/gif',
                        'text/html',
                        'image/png',
                        'image/svs',
                        'text/autosql'
                    ]
                },
                'md5sum': {
                    'title': 'MD5sum',
                    'type': 'string',
                    'format': 'md5sum'
                },
                'size': {
                    'title': 'File size',
                    'type': 'integer'
                },
                'width': {
                    'title': 'Image width',
                    'type': 'integer'
                },
                'height': {
                    'title': 'Image height',
                    'type': 'integer'
                }
            }
        },
        'date_created': {
            'rdfs:subPropertyOf': 'dc:created',
            'title': 'Date created',
            'comment': 'Do not submit, value is assigned by the server. The date the object is created.',
            'type': 'string',
            'anyOf': [{'format': 'date-time'}, {'format': 'date'}],
            'serverDefault': 'now',
            'permission': 'import_items'
        },
        'submitted_by': {
            'rdfs:subPropertyOf': 'dc:creator',
            'title': 'Submitted by',
            'comment': 'Do not submit, value is assigned by the server. The user that created the object.',
            'type': 'string',
            'linkTo': 'User',
            'serverDefault': 'userid',
            'permission': 'import_items'
        }
    }
    assert resolved == expected
    schema['properties'] = {}
    # Status override
    schema['properties']['$merge'] = [
        'mixins.json#/standard_status',
        'mixins.json#/attachment',
        'mixins.json#/submitted',
        'mixins.json#/shared_status',
    ]
    resolved = resolve_merge_refs(schema['properties'], resolver)
    expected = {
        'status': {
            "title": "Status",
            "type": "string",
            "default": "current",
            "enum" : [
                "current",
                "deleted",
                "replaced",
                "disabled"
            ]
        },
        'attachment': {
            'title': 'Document file metadata',
            'type': 'object',
            'additionalProperties': False,
            'formInput': 'file',
            'attachment': True,
            'properties': {
                'download': {
                    'title': 'File Name',
                    'type': 'string'
                },
                'href': {
                    'comment': 'Internal webapp URL for document file',
                    'type': 'string'
                },
                'type': {
                    'title': 'MIME type',
                    'type': 'string',
                    'enum': [
                        'application/pdf',
                        'text/plain',
                        'text/tab-separated-values',
                        'image/jpeg',
                        'image/tiff',
                        'image/gif',
                        'text/html',
                        'image/png',
                        'image/svs',
                        'text/autosql'
                    ]
                },
                'md5sum': {
                    'title': 'MD5sum',
                    'type': 'string',
                    'format': 'md5sum'
                },
                'size': {
                    'title': 'File size',
                    'type': 'integer'
                },
                'width': {
                    'title': 'Image width',
                    'type': 'integer'
                },
                'height': {
                    'title': 'Image height',
                    'type': 'integer'
                }
            }
        },
        'date_created': {
            'rdfs:subPropertyOf': 'dc:created',
            'title': 'Date created',
            'comment': 'Do not submit, value is assigned by the server. The date the object is created.',
            'type': 'string',
            'anyOf': [{'format': 'date-time'}, {'format': 'date'}],
            'serverDefault': 'now',
            'permission': 'import_items'
        },
        'submitted_by': {
            'rdfs:subPropertyOf': 'dc:creator',
            'title': 'Submitted by',
            'comment': 'Do not submit, value is assigned by the server. The user that created the object.',
            'type': 'string',
            'linkTo': 'User',
            'serverDefault': 'userid',
            'permission': 'import_items'
        }
    }
    assert resolved == expected
    schema['properties'] = {}
    # Status override
    schema['properties']['$merge'] = [
        'mixins.json#/standard_status',
        'mixins.json#/attachment',
        'mixins.json#/submitted',
        'mixins.json#/shared_status',
    ]
    schema['properties']['attachment'] = {'something': 'else'}
    schema['properties']['submitted_by'] = {'override': 'value'}
    resolved = resolve_merge_refs(schema['properties'], resolver)
    expected = {
        'status': {
            'title': 'Status',
            'type': 'string',
            'default': 'current',
            'enum': [
                'current',
                'deleted',
                'replaced',
                'disabled'
            ]
        },
        'attachment': {
            'something': 'else'
        },
        'date_created': {
            'rdfs:subPropertyOf': 'dc:created',
            'title': 'Date created',
            'comment': 'Do not submit, value is assigned by the server. The date the object is created.',
            'type': 'string', 'anyOf': [
                {'format': 'date-time'},
                {'format': 'date'}
            ],
            'serverDefault': 'now',
            'permission': 'import_items'
        },
        'submitted_by': {
            'override': 'value'
        }
    }
    assert resolved == expected


def test_schema_utils_fill_in_schema_merge_refs():
    import codecs
    import json
    from pyramid.path import AssetResolver
    from jsonschema import RefResolver
    from snovault.schema_utils import fill_in_schema_merge_refs
    filename = 'snowflakes:schemas/snowball.json'
    utf8 = codecs.getreader('utf-8')
    asset = AssetResolver(
        'snowflakes'
    ).resolve(
        filename
    )
    schema = json.load(
        utf8(
            asset.stream()
        ),
        object_pairs_hook=dict
    )
    assert list(schema['properties'].keys()) == [
        'schema_version',
        'method'
    ]
    resolver = RefResolver('file://' + asset.abspath(), schema)
    # Add ref to something besides mixins.
    schema['properties']['snowball_phone'] = {
        '$merge': 'lab.json#/properties/phone1',
        # Override default
        'default': '999-123-4567',
    }
    resolved = fill_in_schema_merge_refs(
        schema,
        resolver,
    )
    expected = {
        'title': 'Snowball',
        'description': 'Schema for submitting metadata for a Snowball with 1 or more snowflakes', 'comment': 'An snowball is a special case of snowset.',
        '$id': '/profiles/snowball.json',
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'required': ['award', 'lab'],
        'identifyingProperties': ['uuid', 'accession'],
        'additionalProperties': False,
        'mixinProperties': [
            {'$ref': 'mixins.json#/schema_version'},
            {'$ref': 'mixins.json#/uuid'},
            {'$ref': 'mixins.json#/accession'},
            {'$ref': 'mixins.json#/attribution'},
            {'$ref': 'mixins.json#/submitted'},
            {'$ref': 'snowset.json#/properties'}
        ],
        'dependentSchemas': {
            'status': {
                'oneOf': [
                    {
                        'required': ['date_released'],
                        'properties': {
                            'status': {
                                'enum': [
                                    'released', 'revoked'
                                ]
                            }
                        }
                    },
                    {
                        'not': {
                            'properties': {
                                'status': {
                                    'enum': [
                                        'released', 'revoked'
                                    ]
                                }
                            }
                        }
                    }
                ]
            }
        },
        'properties': {
            'schema_version': {
                'default': '2',
                'comment': 'For testing upgrades'
            },
            'method': {
                'title': 'Method',
                'description':
                'Technique used to make snowball.',
                'type': 'string',
                'default': 'hand-packed',
                'enum': [
                    'hand-packed',
                    'scoop-formed',
                    'accreted'
                ]
            },
            'snowball_phone': {
                'title': 'Primary phone number',
                'description': "The lab's primary phone number (with country code).",
                'type': 'string',
                'default': '999-123-4567',
                'format': 'phone'
            }
        },
        'facets': {
            'method': {'title': 'Method'},
            'award.project': {'title': 'Project'},
            'award.rfa': {'title': 'RFA'},
            'status': {'title': 'Snowball status'},
            'snowflakes.type': {'title': 'Flakes'},
            'month_released': {'title': 'Date released'},
            'lab.title': {'title': 'Lab'}
        },
        'columns': {
            'accession': {'title': 'Accession'},
            'method': {'title': 'Method'},
            'lab.title': {'title': 'Lab'},
            'award.project': {'title': 'Project'},
            'status': {'title': 'Status'},
            'snowflakes.length': {'title': 'Number of snowflakes'}
        },
        'boost_values': {
            'accession': 1.0,
            'method': 1.0,
            'snowflakes.type': 1.0,
            'award.title': 1.0,
            'award.project': 1.0,
            'submitted_by.email': 1.0,
            'submitted_by.first_name': 1.0,
            'submitted_by.last_name': 1.0,
            'lab.institute_name': 1.0,
            'lab.institute_label': 1.0,
            'lab.title': 1.0
        }
    }
    assert resolved == expected
    schema['dependentSchemas']['$merge'] = ['snowflake.json#/dependentSchemas']
    resolved = fill_in_schema_merge_refs(
        schema,
        resolver,
    )
    expected = {
        'status': {
            'oneOf': [
                {
                    'required': ['date_released'],
                    'properties': {
                        'status': {
                            'enum': [
                                'released',
                                'revoked'
                            ]
                        }
                    }
                },
                {
                    'not': {
                        'properties': {
                            'status': {
                                'enum': [
                                    'released',
                                    'revoked'
                                ]
                            }
                        }
                    }
                }
            ]
        },
        'external_accession': {
            'not': {
                'required': [
                    'accession'
                ]
            }
        }
    }
    assert resolved['dependentSchemas'] == expected
