from pyramid.security import (
    Allow,
)
from pyramid.view import view_config
from snovault import (
    Item,
    calculated_property,
    collection,
)
from snowflakes.types.base import paths_filtered_by_status
from snovault.attachment import ItemWithAttachment
from snovault.util import Path


def includeme(config):
    config.scan(__name__)
    config.include('.testing_auditor')


@view_config(name='testing-user', request_method='GET')
def user(request):
    return {
        'authenticated_userid': request.authenticated_userid,
        'effective_principals': request.effective_principals,
    }


@view_config(name='testing-allowed', request_method='GET')
def allowed(context, request):
    from pyramid.security import (
        has_permission,
        principals_allowed_by_permission,
    )
    permission = request.params.get('permission', 'view')
    return {
        'has_permission': bool(has_permission(permission, context, request)),
        'principals_allowed_by_permission': principals_allowed_by_permission(context, permission),
    }


@collection(
    'testing-downloads',
    properties={
        'title': 'Test download collection',
        'description': 'Testing. Testing. 1, 2, 3.',
    },
)
class TestingDownload(ItemWithAttachment):
    item_type = 'testing_download'
    schema = {
        'type': 'object',
        'properties': {
            'attachment': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['image/png'],
                    }
                }
            },
            'attachment2': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['image/png'],
                    }
                }
            },
            'attachment3': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['application/json'],
                    }
                }
            },
            'attachment4': {
                'type': 'object',
                'attachment': True,
                'properties': {
                    'type': {
                        'type': 'string',
                        'enum': ['application/json'],
                    }
                }
            }
        }
    }


@collection('testing-link-sources')
class TestingLinkSource(Item):
    item_type = 'testing_link_source'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
            },
            'uuid': {
                'type': 'string',
            },
            'target': {
                'type': 'string',
                'linkTo': 'TestingLinkTarget',
            },
            'status': {
                'type': 'string',
            },
            'condition1': {
                'type': 'boolean',
            },
        },
        'required': ['target'],
        'additionalProperties': False,
    }


@collection('testing-link-targets', unique_key='testing_link_target:name')
class TestingLinkTarget(Item):
    item_type = 'testing_link_target'
    name_key = 'name'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'uuid': {
                'type': 'string',
            },
            'status': {
                'type': 'string',
            },
        },
        'additionalProperties': False,
    }
    rev = {
        'reverse': ('TestingLinkSource', 'target'),
    }
    embedded = [
        'reverse',
    ]
    audit_inherit = ['*']

    @calculated_property(schema={
        "title": "Sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "TestingLinkSource.target",
        },
    })
    def reverse(self, request, reverse):
        return paths_filtered_by_status(request, reverse)


@collection(
    'testing-search-scheams',
    unique_key='testing_search_schema:name'
)
class TestingSearchSchema(Item):
    item_type = 'testing_search_schema'
    name_key = 'name'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'status': {
                'type': 'string',
            },
            'uuid': {
                'title': 'UUID',
                'description': 'Unique identifier',
                'type': 'string',
                'format': 'uuid',
                'permission': 'import_items',
                'requestMethod': 'POST',
            },
            'accession': {
                'title': 'Accession',
                'description': '',
                'type': 'string',
                'format': 'accession',
                'permission': 'import_items'
            },
            'label': {
                'type': 'string',
            }
        },
        'additionalProperties': False,
        'facets': {
            'status': {
                'title': 'Status',
                'open_on_load': True
            },
            'name': {
                'title': 'Name'
            }
        },
        'boost_values': {
            'accession': 1.0,
            'status': 1.0,
            'label': 1.0
        },
        'columns': {
            'accession': {
                'title': 'Accession'
            },
            'status': {
                'title': 'Status'
            }
        }
    }
    audit_inherit = ['*']
    matrix = {
        'x': {
            'group_by': 'label'
        },
        'y': {
            'group_by': ['status', 'name']
        }
    }
    missing_matrix = {
        'x': {
            'group_by': 'label'
        },
        'y': {
            'group_by': ['status', ('name', 'default_name')]
        }
    }
    audit = {
        'audit.ERROR.category': {
            'group_by': 'audit.ERROR.category',
            'label': 'Error'
        },
        'audit.INTERNAL_ACTION.category': {
            'group_by': 'audit.INTERNAL_ACTION.category',
            'label': 'Internal Action'},
        'audit.NOT_COMPLIANT.category': {
            'group_by': 'audit.NOT_COMPLIANT.category',
            'label': 'Not Compliant'
        },
        'audit.WARNING.category': {
            'group_by': 'audit.WARNING.category',
            'label': 'Warning'
        },
        'x': {
            'group_by': 'status', 'label': 'Status'
        }
    }


@collection(
    'testing-post-put-patch',
    acl=[
        (Allow, 'group.submitter', ['add', 'edit', 'view']),
    ],
)
class TestingPostPutPatch(Item):
    item_type = 'testing_post_put_patch'
    schema = {
        'required': ['required'],
        'type': 'object',
        'properties': {
            "schema_version": {
                "type": "string",
                "pattern": "^\\d+(\\.\\d+)*$",
                "requestMethod": [],
                "default": "1",
            },
            "uuid": {
                "title": "UUID",
                "description": "",
                "type": "string",
                "format": "uuid",
                "permission": "import_items",
                "requestMethod": "POST",
            },
            "accession": {
                "title": "Accession",
                "description": "",
                "type": "string",
                "format": "accession",
                "permission": "import_items"
            },
            'required': {
                'type': 'string',
            },
            'simple1': {
                'type': 'string',
                'default': 'simple1 default',
            },
            'simple2': {
                'type': 'string',
                'default': 'simple2 default',
            },
            'protected': {
                # This should be allowed on PUT so long as value is the same
                'type': 'string',
                'default': 'protected default',
                'permission': 'import_items',
            },
            'protected_link': {
                # This should be allowed on PUT so long as the linked uuid is
                # the same
                'type': 'string',
                'linkTo': 'TestingLinkTarget',
                'permission': 'import_items',
            },
        }
    }


@collection('testing-server-defaults')
class TestingServerDefault(Item):
    item_type = 'testing_server_default'
    schema = {
        'type': 'object',
        'properties': {
            "schema_version": {
                "type": "string",
                "pattern": "^\\d+(\\.\\d+)*$",
                "requestMethod": [],
            },
            "uuid": {
                "title": "UUID",
                "description": "",
                "type": "string",
                "format": "uuid",
                "permission": "import_items",
                "requestMethod": "POST",
            },
            'user': {
                'serverDefault': 'userid',
                'linkTo': 'User',
                'type': 'string',
            },
            'now': {
                'serverDefault': 'now',
                'format': 'date-time',
                'type': 'string',
            },
            'accession': {
                'serverDefault': 'accession',
                'accessionType': 'SS',
                'format': 'accession',
                'type': 'string',
            },
        }
    }


@collection('testing-bad-accession')
class TestingBadAccession(Item):
    item_type = 'testing_bad_accession'
    schema = {
        'type': 'object',
        'properties': {
            "schema_version": {
                "type": "string",
                "pattern": "^\\d+(\\.\\d+)*$",
            },
            "uuid": {
                "title": "UUID",
                "description": "",
                "type": "string",
                "format": "uuid",
                "permission": "import_items",
                "requestMethod": "POST",
            },
            'thing': {
                'type': "number",
                'default': 3,
            },
            'user': {
                'serverDefault': 'userid',
                'linkTo': 'User',
                'type': 'string',
            },
            'now': {
                'serverDefault': 'now',
                'format': 'date-time',
                'type': 'string',
            },
            'accession': {
                'serverDefault': 'accession',
                'accessionType': 'SS',
                'format': 'accession',
                'type': 'string',
            },
        }
    }


@collection('testing-dependencies')
class TestingDependencies(Item):
    item_type = 'testing_dependencies'
    schema = {
        'type': 'object',
        'dependencies': {
            'dep1': ['dep2'],
            'dep2': ['dep1'],
        },
        'properties': {
            'dep1': {
                'type': 'string',
            },
            'dep2': {
                'type': 'string',
                'enum': ['dep2'],
            },
        }
    }


@view_config(name='testing-render-error', request_method='GET')
def testing_render_error(request):
    return {
        '@type': ['TestingRenderError', 'Item'],
        '@id': request.path,
        'title': 'Item triggering a render error',
    }


@view_config(context=TestingPostPutPatch, name='testing-retry')
def testing_retry(context, request):
    from sqlalchemy import inspect
    from transaction.interfaces import TransientError

    model = context.model
    attempt = request.environ.get('retry.attempts')

    if attempt == 0:
        raise TransientError()

    return {
        'retry.attempts': attempt,
        'detached': inspect(model).detached,
    }


@collection('testing-custom-embed-sources')
class TestingCustomEmbedSource(Item):
    item_type = 'testing_custom_embed_source'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
            },
            'uuid': {
                'type': 'string',
            },
            'target': {
                'type': 'string',
                'linkTo': 'TestingCustomEmbedTarget',
            },
            'status': {
                'type': 'string',
            },
            'condition1': {
                'type': 'boolean',
            },
        },
        'required': ['target'],
        'additionalProperties': False,
    }


@collection('testing-custom-embed-targets', unique_key='testing_custom_embed_target:name')
class TestingCustomEmbedTarget(Item):
    item_type = 'testing_custom_embed_target'
    name_key = 'name'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'uuid': {
                'type': 'string',
            },
            'status': {
                'type': 'string',
            },
        },
        'additionalProperties': False,
    }
    rev = {
        'reverse': ('TestingCustomEmbedSource', 'target'),
    }
    embedded_with_frame = [
        Path('reverse'),
        Path('filtered_reverse', include=['uuid', 'status']),
        Path('filtered_reverse1', exclude=['uuid', '@type']),
        Path('reverse_uncalculated', frame='@@object?skip_calculated=true'),
    ]
    audit_inherit = ['*']

    @calculated_property(schema={
        "title": "Sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "TestingCustomEmbedSource.target",
        },
    })
    def reverse(self, request, reverse):
        return paths_filtered_by_status(request, reverse)


    @calculated_property(schema={
        "title": "Filtered sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "TestingCustomEmbedSource.target",
        },
    })
    def filtered_reverse(self, request, reverse):
        return paths_filtered_by_status(request, reverse)

    @calculated_property(schema={
        "title": "Filtered sources1",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "TestingCustomEmbedSource.target",
        },
    })
    def filtered_reverse1(self, request, reverse):
        return paths_filtered_by_status(request, reverse)

    @calculated_property(schema={
        "title": "Uncalculated sources",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "TestingCustomEmbedSource.target",
        },
    })
    def reverse_uncalculated(self, request, reverse):
        return paths_filtered_by_status(request, reverse)
