import re
from urllib.parse import urlencode
from pyramid.view import view_config
from pyramid.httpexceptions import HTTPBadRequest
from pyramid.security import effective_principals
from elasticsearch.helpers import scan
from snovault import (
    AbstractCollection,
    TYPES,
)
from snovault.viewconfigs.searchpageview import SearchPageView
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.resource_views import collection_view_listing_db
from snovault.helpers.helper import (
    sort_query,
    get_pagination,
    get_filtered_query,
    prepare_search_term,
    set_sort_order,
    get_search_fields,
    list_visible_columns_for_schemas,
    list_result_fields,
    set_filters,
    set_facets,
    format_results,
    format_facets,
    normalize_query,
    iter_long_json
)


CHAR_COUNT = 32


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('report', '/report{slash:/?}')
    config.scan(__name__)


SANITIZE_SEARCH_STRING_RE = re.compile(r'[\\\+\-\&\|\!\(\)\{\}\[\]\^\~\:\/\\\*\?]')

AUDIT_FACETS = [
    ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
    ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
    ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
    ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
]


DEFAULT_DOC_TYPES = [
    'Lab',
    'Snowset',
    'Snowball',
    'Snowfort',
    'Snowflake',
]


@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False):
    search = SearchPageView(context, request, search_type. return_generator)
    return search.preprocess_view()

def iter_search_results(context, request):
    return search(context, request, return_generator=True)


@view_config(context=AbstractCollection, permission='list', request_method='GET',
             name='listing')
def collection_view_listing_es(context, request):
    # Switch to change summary page loading options
    if request.datastore != 'elasticsearch':
        return collection_view_listing_db(context, request)

    return search(context, request, context.type_info.name)


@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):
    doc_types = request.params.getall('type')
    if len(doc_types) != 1:
        msg = 'Report view requires specifying a single type.'
        raise HTTPBadRequest(explanation=msg)

    # schemas for all types
    types = request.registry[TYPES]

    # Get the subtypes of the requested type
    try:
        sub_types = types[doc_types[0]].subtypes
    except KeyError:
        # Raise an error for an invalid type
        msg = "Invalid type: " + doc_types[0]
        raise HTTPBadRequest(explanation=msg)

    # Raise an error if the requested type has subtypes.
    if len(sub_types) > 1:
        msg = 'Report view requires a type with no child types.'
        raise HTTPBadRequest(explanation=msg)

    # Ignore large limits, which make `search` return a Response
    # -- UNLESS we're being embedded by the download_report view
    from_, size = get_pagination(request)
    if ('limit' in request.GET and request.__parent__ is None
            and (size is None or size > 1000)):
        del request.GET['limit']
    # Reuse search view
    res = search(context, request)

    # change @id, @type, and views
    res['views'][0] = {
        'href': res['@id'],
        'title': 'View results as list',
        'icon': 'list-alt',
    }
    search_base = normalize_query(request)
    res['@id'] = '/report/' + search_base
    # TODO add this back one day
    # res['download_tsv'] = request.route_path('report_download') + search_base
    res['title'] = 'Report'
    res['@type'] = ['Report']
    return res
