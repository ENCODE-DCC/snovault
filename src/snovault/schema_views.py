from pyramid.httpexceptions import HTTPNotFound
from pyramid.view import view_config
from .etag import etag_app_version_effective_principals
from .interfaces import (
    COLLECTIONS,
    TYPES,
)
from .util import mutated_schema


def includeme(config):
    config.add_route('schemas', '/profiles{slash:/?}')
    config.add_route('schema', '/profiles/{type_name}.json')
    config.add_route('schemap', '/profiles/{type_name}{slash:/?}')
    config.add_route('schemas_map', '/profiles-map/')
    config.add_route('schemas_titles', '/profiles-titles/')
    config.scan(__name__)


def _apply_permission(collection, request):
    def mutator(schema):
        if 'permission' in schema:
            if not request.has_permission(schema['permission'], collection):
                schema = schema.copy()
                schema['readonly'] = True
        return schema
    return mutator


def _annotated_schema(type_info, request):
    schema = type_info.schema.copy()
    schema['@type'] = ['JSONSchema']
    if type_info.factory is None:
        return schema

    collection = request.registry[COLLECTIONS][type_info.name]
    return mutated_schema(
        schema,
        _apply_permission(collection, request),
    )


@view_config(route_name='schema', request_method='GET',
             decorator=etag_app_version_effective_principals)
@view_config(route_name='schemap', request_method='GET',
             decorator=etag_app_version_effective_principals)
def schema(context, request):
    type_name = request.matchdict['type_name']
    types = request.registry[TYPES]
    try:
        type_info = types[type_name]
    except KeyError:
        raise HTTPNotFound(type_name)

    return _annotated_schema(type_info, request)


@view_config(route_name='schemas', request_method='GET',
             decorator=etag_app_version_effective_principals)
def schemas(context, request):
    types = request.registry[TYPES]
    schemas = {}
    for type_info in types.by_item_type.values():
        name = type_info.name
        schemas[name] = _annotated_schema(type_info, request)

    schemas['_subtypes'] = subtypes = {}
    schemas['@type'] = ['JSONSchemas']
    for name, type_info in types.abstract.items():
        subtypes[name] = type_info.subtypes

    return schemas


@view_config(route_name='schemas_map', request_method='GET',
             decorator=etag_app_version_effective_principals)
def schemas_map(context, request):
    types = request.registry[TYPES]
    profiles_map = {}
    for type_info in types.by_item_type.values():
        if 'id' in type_info.schema:
            profiles_map[type_info.name] = type_info.schema['id']
    profiles_map['@type'] = ['JSONSchemas']
    return profiles_map


@view_config(route_name='schemas_titles', request_method='GET',
             decorator=etag_app_version_effective_principals)
def schemas_titles(context, request):  # pylint: disable=unused-argument
    '''Return mapping of all schema @types and their corresponding titles'''
    types = request.registry[TYPES]
    profiles_titles = {
        type_info.name: type_info.schema['title']
        for type_info in types.by_item_type.values()
        if 'title' in type_info.schema
    }
    profiles_titles['@type'] = ['JSONSchemas']
    return profiles_titles
