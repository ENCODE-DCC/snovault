""" Cached views used when model was pulled from elasticsearch.
"""

from itertools import chain
from pyramid.httpexceptions import HTTPForbidden
from pyramid.view import view_config
from .interfaces import ICachedItem


def includeme(config):
    config.scan(__name__)


@view_config(context=ICachedItem, request_method='GET', name='embedded')
def cached_view_embedded(context, request):
    source = context.model.source
    allowed = set(source['principals_allowed']['view'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return source['embedded']


_skip_fields = ['@type', 'principals_allowed']


def filter_embedded(embedded, effective_principals):
    # handle dictionary
    if isinstance(embedded, dict):
        if 'principals_allowed' in embedded.keys():
            obj_princ = embedded.get('principals_allowed')
            allowed = set(obj_princ['view'])
            if allowed.isdisjoint(effective_principals):
                embedded = {'error': 'no view permissions'}
                return embedded

        for name, obj in embedded.items():
            if isinstance(obj, (dict, list)) and name not in _skip_fields:
                embedded[name] = filter_embedded(obj, effective_principals)

    # handle array
    elif isinstance(embedded, list):
        for idx, item in enumerate(embedded):
            embedded[idx] = filter_embedded(item, effective_principals)

    # default just return the sucker
    return embedded


@view_config(context=ICachedItem, request_method='GET', name='object')
def cached_view_object(context, request):
    source = context.model.source
    allowed = set(source['principals_allowed']['view'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return source['object']


@view_config(context=ICachedItem, request_method='GET', name='audit')
def cached_view_audit(context, request):
    source = context.model.source
    allowed = set(source['principals_allowed']['audit'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    return {
        '@id': source['object']['@id'],
        'audit': source['audit'],
    }


@view_config(context=ICachedItem, request_method='GET', name='audit-self')
def cached_view_audit_self(context, request):
    source = context.model.source
    allowed = set(source['principals_allowed']['audit'])
    if allowed.isdisjoint(request.effective_principals):
        raise HTTPForbidden()
    path = source['object']['@id']
    return {
        '@id': path,
        'audit': [a for a in chain(*source['audit'].values()) if a['path'] == path],
    }
