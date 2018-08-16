from pyramid.security import (
    Authenticated,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import resource_path
from pyramid.view import view_config
from .resources import Item
from .authentication import calc_principals


def includeme(config):
    config.scan(__name__)


# really simple exception to know when the sid check fails
class SidException(Exception):
    pass


@view_config(context=Item, name='index-data', permission='index', request_method='GET')
def item_index_data(context, request):
    uuid = str(context.uuid)
    properties = context.upgrade_properties()

    # if we want to check an sid, it should be set as a query param
    sid_check = request.params.get('sid', None)
    if sid_check:
        try:
            sid_check = int(sid_check)
        except ValueError:
            raise ValueError('sid parameter must be an integer. Provided sid: %s' % sid)
        if context.sid < sid_check:
            raise SidException('sid from the query (%s) is greater than that on context (%s). Bailing.' % (sid_check, context.sid))

    # ES 2 and up don't allow dots in links. Update these to use ~s
    new_links = {}
    for key, val in context.links(properties).items():
        new_links['~'.join(key.split('.'))] = val
    links = new_links
    unique_keys = context.unique_keys(properties)

    principals_allowed = calc_principals(context)
    path = resource_path(context)
    paths = {path}
    collection = context.collection

    if collection.unique_key in unique_keys:
        paths.update(
            resource_path(collection, key)
            for key in unique_keys[collection.unique_key])

    for base in (collection, request.root):
        for key_name in ('accession', 'alias'):
            if key_name not in unique_keys:
                continue
            paths.add(resource_path(base, uuid))
            paths.update(
                resource_path(base, key)
                for key in unique_keys[key_name])

    path = path + '/'
    # setting _indexing_view enables the embed_cache and cause population of
    # request._linked_uuids and request._rev_linked_uuids_by_item

    request._indexing_view = True
    # reset these properties
    request._linked_uuids = set()
    request._audit_uuids = set()
    request._rev_linked_uuids_by_item = {}
    # since request._indexing_view is set to True in indexer.py,
    # all embeds (including subrequests) below will use the embed cache
    embedded = request.invoke_view(path, '@@embedded')
    # get _linked and _rev_linked uuids from the request before @@audit views add to them
    linked_uuids = sorted(request._linked_uuids.copy())
    rev_linked_by_item = request._rev_linked_uuids_by_item.copy()
    # find uuids traversed that rev link to this item
    rev_linked_to_me = sorted(set([id for id in rev_linked_by_item if uuid in rev_linked_by_item[id]]))
    # set the uuids we want to audit on
    request._audit_uuids = linked_uuids
    audit = request.invoke_view(path, '@@audit')['audit']
    obj = request.invoke_view(path, '@@object')
    document = {
        'audit': audit,
        'embedded': embedded,
        'linked_uuids': linked_uuids,
        'item_type': context.type_info.item_type,
        'links': links,
        'object': obj,
        'paths': sorted(paths),
        'principals_allowed': principals_allowed,
        'properties': properties,
        'propsheets': {
            name: context.propsheets[name]
            for name in context.propsheets.keys() if name != ''
        },
        'sid': context.sid,
        'unique_keys': unique_keys,
        'uuid': uuid,
        'uuids_rev_linked_to_me': rev_linked_to_me
    }

    return document
