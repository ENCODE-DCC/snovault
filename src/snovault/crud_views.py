from past.builtins import basestring
from pyramid.settings import asbool
from pyramid.traversal import (
    find_resource,
)
from pyramid.view import view_config
from uuid import (
    UUID,
    uuid4,
)
from .etag import if_match_tid
from .interfaces import (
    COLLECTIONS,
    CONNECTION,
    STORAGE,
    Created,
    BeforeModified,
    AfterModified,
)
from .resources import (
    Collection,
    Item,
)
from .validation import ValidationFailure
from .validators import (
    no_validate_item_content_patch,
    no_validate_item_content_post,
    no_validate_item_content_put,
    validate_item_content_patch,
    validate_item_content_post,
    validate_item_content_put,
)


def includeme(config):
    config.scan(__name__)


def split_child_props(type_info, properties):
    propname_children = {}
    item_properties = properties.copy()
    if type_info.schema_rev_links:
        for key, spec in type_info.schema_rev_links.items():
            if key in item_properties:
                propname_children[key] = item_properties.pop(key)
    return item_properties, propname_children


def update_children(context, request, propname_children):
    registry = request.registry
    conn = registry[CONNECTION]
    collections = registry[COLLECTIONS]
    schema_rev_links = context.type_info.schema_rev_links

    for propname, children in propname_children.items():
        link_type, link_attr = schema_rev_links[propname]
        child_collection = collections[link_type]
        found = set()

        # Add or update children included in properties
        for i, child_props in enumerate(children):
            if isinstance(child_props, basestring):  # IRI of (existing) child
                child = find_resource(child_collection, child_props)
            else:
                child_props = child_props.copy()
                child_props[link_attr] = str(context.uuid)
                if 'uuid' in child_props:  # update existing child
                    child_id = child_props.pop('uuid')
                    child = conn.get_by_uuid(child_id)
                    if not request.has_permission('edit', child):
                        msg = u'edit forbidden to %s' % request.resource_path(child)
                        raise ValidationFailure('body', [propname, i], msg)
                    try:
                        update_item(child, request, child_props)
                    except ValidationFailure as e:
                        e.location = [propname, i] + e.location
                        raise
                else:  # add new child
                    if not request.has_permission('add', child_collection):
                        msg = u'edit forbidden to %s' % request.resource_path(child)
                        raise ValidationFailure('body', [propname, i], msg)
                    child = create_item(child_collection.type_info, request, child_props)
            found.add(child.uuid)

        # Remove existing children that are not in properties
        for link_uuid in context.get_rev_links(propname):
            if link_uuid in found:
                continue
            child = conn.get_by_uuid(link_uuid)
            if not request.has_permission('visible_for_edit', child):
                continue
            if not request.has_permission('edit', child):
                msg = u'edit forbidden to %s' % request.resource_path(child)
                raise ValidationFailure('body', [propname, i], msg)
            try:
                delete_item(child, request)
            except ValidationFailure as e:
                e.location = [propname, i] + e.location
                raise


def create_item(type_info, request, properties, sheets=None):
    registry = request.registry
    item_properties, propname_children = split_child_props(type_info, properties)

    if 'uuid' in item_properties:
        try:
            nonvalidated_uuid = item_properties.pop('uuid')
            uuid = UUID(nonvalidated_uuid)
        except ValueError as e:
            raise ValueError(str(e) + ': ' + str(nonvalidated_uuid))
    else:
        uuid = uuid4()

    item = type_info.factory.create(registry, uuid, item_properties, sheets)
    registry.notify(Created(item, request))

    if propname_children:
        update_children(item, request, propname_children)
    return item


def update_item(context, request, properties, sheets=None):
    registry = request.registry
    item_properties, propname_children = split_child_props(context.type_info, properties)

    registry.notify(BeforeModified(context, request))
    context.update(item_properties, sheets)
    registry.notify(AfterModified(context, request))

    if propname_children:
        update_children(context, request, propname_children)


def delete_item(context, request):
    properties = context.properties.copy()
    properties['status'] = 'deleted'
    update_item(context, request, properties)
    return True


def delete_item_from_database(context, request):
    item_type = context.collection.type_info.item_type
    item_uuid = str(context.uuid)
    request.registry[STORAGE].delete_by_uuid(item_uuid, item_type)
    return True


def render_item(request, context, render, return_uri_also=False):
    if render == 'uuid':
        item_uri = '/%s' % context.uuid
    else:
        item_uri = request.resource_path(context)

    if asbool(render) is True:
        rendered = request.embed(item_uri, '@@object', as_user=True)
    else:
        rendered = item_uri
    return (rendered, item_uri) if return_uri_also else rendered

@view_config(context=Collection, permission='add', request_method='POST',
             validators=[validate_item_content_post])
@view_config(context=Collection, permission='add_unvalidated', request_method='POST',
             validators=[no_validate_item_content_post],
             request_param=['validate=false'])
def collection_add(context, request, render=None):
    if render is None:
        render = request.params.get('render', True)

    item = create_item(context.type_info, request, request.validated)
    rendered, item_uri = render_item(request, item, render, True)

    request.response.status = 201
    request.response.location = item_uri
    result = {
        'status': 'success',
        '@type': ['result'],
        '@graph': [rendered],
    }
    return result


@view_config(context=Item, permission='edit', request_method='PUT',
             validators=[validate_item_content_put], decorator=if_match_tid)
@view_config(context=Item, permission='edit', request_method='PATCH',
             validators=[validate_item_content_patch], decorator=if_match_tid)
@view_config(context=Item, permission='edit_unvalidated', request_method='PUT',
             validators=[no_validate_item_content_put],
             request_param=['validate=false'], decorator=if_match_tid)
@view_config(context=Item, permission='edit_unvalidated', request_method='PATCH',
             validators=[no_validate_item_content_patch],
             request_param=['validate=false'], decorator=if_match_tid)
def item_edit(context, request, render=None):
    """ This handles both PUT and PATCH, difference is the validator

    PUT - replaces the current properties with the new body
    PATCH - updates the current properties with those supplied.
    Note validators will handle the PATH ?delete_fields parameter if you want
    field to be deleted
    """
    if render is None:
        render = request.params.get('render', True)

    # This *sets* the property sheet
    update_item(context, request, request.validated)
    rendered = render_item(request, context, render)
    request.response.status = 200
    result = {
        'status': 'success',
        '@type': ['result'],
        '@graph': [rendered],
    }
    return result


@view_config(context=Item, permission='edit', request_method='DELETE', decorator=if_match_tid)
def item_delete_full(context, request, render=None):
    # possibly temporary fix to check if user is admin
    user_details = request.user_info.get('details', {})
    if 'admin' not in user_details.get('groups', []):
        msg = u'Must be admin to fully delete items.'
        raise ValidationFailure('body', ['userid'], msg)

    delete_from_database = asbool(request.GET and request.GET.get('delete_from_database'))
    uuid = str(context.uuid)

    if delete_from_database:
        # Delete entirely - WARNING USE WITH CAUTION - DELETES PERMANENTLY
        result = delete_item_from_database(context, request)
        if result:
            return {
                'status': 'success',
                '@type': ['result'],
                'notification' : 'Permanently deleted ' + uuid,
                '@graph': [uuid]
            }
    else:
        result = delete_item(context, request)
        if result:
            return {
                'status': 'success',
                '@type': ['result'],
                'notification' : 'Set status of ' + uuid + ' to deleted',
                '@graph': [ render_item(request, context, render) ]
            }

    #TODO: Throw error of some sort if no exceptions?

    return {
        'status': 'failure',
        '@type': ['result'],
        '@graph': [uuid]
    }
