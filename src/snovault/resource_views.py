import sys
from future.utils import (
    raise_with_traceback,
    itervalues,
)
from itertools import islice
from past.builtins import basestring
from pyramid.exceptions import PredicateMismatch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPMovedPermanently
from pyramid.settings import asbool
from pyramid.view import (
    render_view_to_response,
    view_config,
)
from urllib.parse import urlencode
from snosearch.fields import AllResponseField
from snosearch.fields import CollectionSearchWithFacetsResponseField
from snosearch.fields import ColumnsResponseField
from snosearch.fields import CollectionClearFiltersResponseField
from snosearch.fields import FiltersResponseField
from snosearch.fields import IDResponseField
from snosearch.fields import NotificationResponseField
from snosearch.parsers import ParamsParser
from snosearch.parsers import QueryString
from snosearch.responses import FieldedResponse

from snovault.elasticsearch.searches.fields import PassThroughResponseField

from .calculated import calculate_properties
from .calculated import calculate_select_properties
from .calculated import calculate_filtered_properties
from .calculated import _should_render_property
from .etag import etag_tid
from .interfaces import CONNECTION
from .elasticsearch.interfaces import ELASTIC_SEARCH
from .resources import (
    AbstractCollection,
    Item,
    Root,
)
from .util import expand_path


def includeme(config):
    config.scan(__name__)


def remove_item_keys(item, request):
    """Remove keys specified in query from item"""
    removed_fields = request.params.getall('remove')
    for field in removed_fields:
        if field in item:
            del item[field]
    return item


def collection_view_listing_db_with_additional_properties(context, request, additional_properties):
    result = {}

    frame = request.params.get('frame', 'columns')

    limit = request.params.get('limit', 25)
    if limit in ('', 'all'):
        limit = None
    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            limit = 25

    items = (
        item for item in itervalues(context)
        if request.has_permission('view', item)
    )

    if limit is not None:
        items = islice(items, limit)

    result['@graph'] = [
        remove_item_keys(request.embed(request.resource_path(item, '@@' + frame)), request)
        for item in items
    ]

    if limit is not None and len(result['@graph']) == limit:
        params = [(k, v) for k, v in request.params.items() if k != 'limit']
        params.append(('limit', 'all'))
        result['all'] = '%s?%s' % (request.resource_path(context), urlencode(params))

    result.update(additional_properties)

    return result


@view_config(context=AbstractCollection, permission='list', request_method='GET',
             name='listing_db')
def collection_view_listing_db(context, request):
    return collection_view_listing_db_with_additional_properties(context, request, {})


def collection_view_listing_es_with_additional_properties(context, request, additional_properties):
    if not hasattr(request, 'datastore') or request.datastore != ELASTIC_SEARCH:
        return collection_view_listing_db_with_additional_properties(context, request, additional_properties)
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            IDResponseField(),
            CollectionSearchWithFacetsResponseField(),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            CollectionClearFiltersResponseField(),
            ColumnsResponseField(),
            PassThroughResponseField(
                values_to_pass_through=additional_properties,
            ),
        ]
    )
    return fr.render()


@view_config(context=AbstractCollection, permission='list', request_method='GET', name='listing')
def collection_view_listing_es(context, request):
    return collection_view_listing_es_with_additional_properties(context, request, {})


@view_config(context=Root, request_method='GET', name='page')
def home(context, request):
    properties = request.embed(request.resource_path(context), '@@object')
    calculated = calculate_properties(context, request, properties, category='page')
    properties.update(calculated)
    return properties


@view_config(context=Root, request_method='GET', name='object')
@view_config(context=AbstractCollection, permission='list', request_method='GET', name='object')
def collection_view_object(context, request):
    properties = context.__json__(request)
    calculated = calculate_properties(context, request, properties)
    properties.update(calculated)
    return properties


@view_config(context=AbstractCollection, permission='list', request_method='GET', name='page')
def collection_list(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object')
    calculated = calculate_properties(context, request, properties, category='page')
    properties.update(calculated)

    if request.query_string:
        properties['@id'] += '?' + request.query_string

    # Must check if canonical redirect needed before streaming response.
    if request.path_qs != properties['@id']:
        raise HTTPMovedPermanently(location=properties['@id'])

    return collection_view_listing_es_with_additional_properties(
        context,
        request,
        properties,
    )


@view_config(context=Root, request_method='GET')
@view_config(context=AbstractCollection, permission='list', request_method='GET')
@view_config(context=Item, permission='view', request_method='GET')
def item_view(context, request):
    frame = request.params.get('frame', 'page')
    if getattr(request, '__parent__', None) is None:
        # We need the response headers from non subrequests
        try:
            response = render_view_to_response(context, request, name=frame)
        except PredicateMismatch:
            # Avoid this view emitting PredicateMismatch
            exc_class, exc, tb = sys.exc_info()
            exc.__class__ = HTTPNotFound
            raise_with_traceback(exc, tb)
        else:
            if response is None:
                raise HTTPNotFound('?frame=' + frame)
            return response
    path = request.resource_path(context, '@@' + frame)
    if request.query_string:
        path += '?' + request.query_string
    return request.embed(path, as_user=True)


def item_links(context, request):
    # This works from the schema rather than the links table
    # so that upgrade on GET can work.
    properties = context.__json__(request)
    for path in context.type_info.schema_links:
        uuid_to_path(request, properties, path)
    return properties


def uuid_to_path(request, obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if remaining:
        if isinstance(value, list):
            for v in value:
                uuid_to_path(request, v, remaining)
        else:
            uuid_to_path(request, value, remaining)
        return
    conn = request.registry[CONNECTION]
    if isinstance(value, list):
        obj[name] = [
            request.resource_path(conn[v])
            for v in value
        ]
    else:
        obj[name] = request.resource_path(conn[value])


@view_config(context=Item, permission='view', request_method='GET',
             name='object')
def item_view_object(context, request):
    """ Render json structure

    1. Fetch stored properties, possibly upgrading.
    2. Link canonicalization (overwriting uuids.)
    3. Calculated properties (including reverse links.)
    """
    properties = item_links(context, request)
    if not asbool(request.params.get('skip_calculated')):
        calculated = calculate_properties(context, request, properties)
        properties.update(calculated)
    return properties


@view_config(
    context=Item,
    permission='view',
    request_method='GET',
    name='object_with_select_calculated_properties'
)
def item_view_object_with_select_calculated_properties(context, request):
    properties = item_links(context, request)
    qs = QueryString(request)
    select_properties = qs.param_values_to_list(
        params=qs.get_field_filters()
    )
    calculated = calculate_select_properties(
        context,
        request,
        ns=properties,
        select_properties=select_properties
        )
    properties.update(calculated)
    return properties


@view_config(
    context=Item,
    permission='view',
    request_method='GET',
    name='filtered_object'
)
def item_view_filtered_object(context, request):
    properties = item_links(context, request)
    qs = QueryString(request)
    include = qs.param_values_to_list(
        params=qs.get_key_filters(
            key='include'
        )
    ) or None
    exclude = qs.param_values_to_list(
        params=qs.get_key_filters(
            key='exclude'
        )
    ) or None
    calculated = calculate_filtered_properties(
        context,
        request,
        ns=properties,
        include=include,
        exclude=exclude,
    )
    properties.update(calculated)
    return {
        k: v
        for k, v in properties.items()
        if _should_render_property(include, exclude, k)
    }


@view_config(context=Item, permission='view', request_method='GET',
             name='embedded')
def item_view_embedded(context, request):
    item_path = request.resource_path(context)
    properties = request.embed(item_path, '@@object')
    for path in context.embedded:
        expand_path(request, properties, path)
    for path in context.embedded_with_frame:
        path.expand(request, properties)
    return properties


@view_config(context=Item, permission='view', request_method='GET',
             name='page')
def item_view_page(context, request):
    item_path = request.resource_path(context)
    properties = request.embed(item_path, '@@embedded')
    calculated = calculate_properties(context, request, properties, category='page')
    properties.update(calculated)
    return properties


@view_config(context=Item, permission='expand', request_method='GET',
             name='expand')
def item_view_expand(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object')
    for path in request.params.getall('expand'):
        expand_path(request, properties, path)
    return properties


def expand_column(request, obj, subset, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not remaining:
        subset[name] = value
        return
    if isinstance(value, list):
        if name not in subset:
            subset[name] = [{} for i in range(len(value))]
        for index, member in enumerate(value):
            if not isinstance(member, dict):
                member = request.embed(member, '@@object')
            expand_column(request, member, subset[name][index], remaining)
    else:
        if name not in subset:
            subset[name] = {}
        if not isinstance(value, dict):
            value = request.embed(value, '@@object')
        expand_column(request, value, subset[name], remaining)


@view_config(context=Item, permission='view', request_method='GET',
             name='columns')
def item_view_columns(context, request):
    path = request.resource_path(context)
    properties = request.embed(path, '@@object')
    if context.schema is None or 'columns' not in context.schema:
        return properties

    subset = {
        '@id': properties['@id'],
        '@type': properties['@type'],
    }

    for column in context.schema['columns']:
        path = column.split('.')
        if path[-1] == 'length':
            path.pop()
        if path:
            expand_column(request, properties, subset, path)

    return subset


@view_config(context=Item, permission='view_raw', request_method='GET',
             name='raw')
def item_view_raw(context, request):
    if asbool(request.params.get('upgrade', True)):
        return context.upgrade_properties()
    return context.properties


@view_config(context=Item, permission='edit', request_method='GET',
             name='edit', decorator=etag_tid)
def item_view_edit(context, request):
    conn = request.registry[CONNECTION]
    properties = item_links(context, request)
    schema_rev_links = context.type_info.schema_rev_links

    for propname in schema_rev_links:
        properties[propname] = sorted(
            request.resource_path(child)
            for child in (
                conn.get_by_uuid(uuid) for uuid in context.get_rev_links(propname)
            ) if request.has_permission('visible_for_edit', child)
        )

    return properties


@view_config(context=Item, permission='edit', request_method='GET',
             name='form', decorator=etag_tid)
def item_view_form(context, request):
    properties = item_view_edit(context, request)
    properties['@type'] = context.jsonld_type()
    return properties
