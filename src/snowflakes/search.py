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
    search_result_actions,
    format_facets,
    normalize_query,
    iter_long_json
)


CHAR_COUNT = 32


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('report', '/report{slash:/?}')
    config.scan(__name__)


sanitize_search_string_re = re.compile(r'[\\\+\-\&\|\!\(\)\{\}\[\]\^\~\:\/\\\*\?]')

audit_facets = [
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
def search(context, request, return_generator=False):
    """
    Search view connects to ElasticSearch and returns the results
    """
    # sets up ES and checks permissions/principles

    # gets schemas for all types
    types = request.registry[TYPES]
    search_base = normalize_query(request)
    result = {
        '@context': request.route_path('jsonld_context'),
        '@id': '/search/' + search_base,
        '@type': ['Search'],
        'title': 'Search',
        'filters': [],
    }
    principals = effective_principals(request)
    es = request.registry[ELASTIC_SEARCH]
    es_index = RESOURCES_INDEX
    search_audit = request.has_permission('search_audit')


    # extract from/size from query parameters
    from_, size = get_pagination(request)

    # looks at searchTerm query parameter, sets to '*' if none, and creates antlr/lucene query for fancy stuff
    search_term = prepare_search_term(request)

    ## converts type= query parameters to list of doc_types to search, "*" becomes super class Item
    if (hasattr(context, 'type_info') and
            hasattr(context.type_info, 'name')
            and context.type_info.name):
        doc_types = [context.type_info.name]
    else:
        doc_types = request.params.getall('type')
        if '*' in doc_types:
            doc_types = ['Item']

    # Normalize to item_type
    try:
        doc_types = sorted({types[name].name for name in doc_types})
    except KeyError:
        # Check for invalid types
        bad_types = [t for t in doc_types if t not in types]
        msg = "Invalid type: {}".format(', '.join(bad_types))
        raise HTTPBadRequest(explanation=msg)

    # Clear Filters path -- make a path that clears all non-datatype filters.
    # this saves the searchTerm when you click clear filters
    # http://stackoverflow.com/questions/16491988/how-to-convert-a-list-of-strings-to-a-query-string#answer-16492046
    searchterm_specs = request.params.getall('searchTerm')
    searchterm_only = urlencode([("searchTerm", searchterm) for searchterm in searchterm_specs])
    if searchterm_only:
        # Search term in query string; clearing keeps that
        clear_qs = searchterm_only
    else:
        # Possibly type(s) in query string
        clear_qs = urlencode([("type", typ) for typ in doc_types])
    result['clear_filters'] = request.route_path('search', slash='/') + (('?' + clear_qs) if clear_qs else '')

    # Building query for filters
    if not doc_types:
        # For form editing embedded searches
        if request.params.get('mode') == 'picker':
            doc_types = ['Item']
        # For /search/ with no type= use defalts
        else:
            doc_types = DEFAULT_DOC_TYPES
    else:
        # TYPE filters that were set by UI for labeling, only seen with >1 types
        # Probably this is why filtering Items with subclasses doesn't work right
        # i.e., search/?type=Dataset   Type is not a regular filter/facet.
        for item_type in doc_types:
            ti = types[item_type]
            qs = urlencode([
                (k.encode('utf-8'), v.encode('utf-8'))
                for k, v in request.params.items() if not (k == 'type' and types['Item' if v == '*' else v] is ti)
            ])
            result['filters'].append({
                'field': 'type',
                'term': ti.name,
                'remove': '{}?{}'.format(request.path, qs)
            })

        # Add special views like Report and Matrix if search is a single type
        if len(doc_types) == 1:
            result['views'] = views = []
            views.append({
                'href': request.route_path('report', slash='/') + search_base,
                'title': 'View tabular report',
                'icon': 'table',
            })

    search_fields, highlights = get_search_fields(request, doc_types)

    # Builds filtered query which supports multiple facet selection
    query = get_filtered_query(search_term,
                               search_fields,
                               sorted(list_result_fields(request, doc_types)),
                               principals,
                               doc_types)

    #  Columns is used in report view
    schemas = [types[doc_type].schema for doc_type in doc_types]
    columns = list_visible_columns_for_schemas(request, schemas)
    # and here it is attached to the result for the UI
    if columns:
        result['columns'] = columns

    # If no text search, use match_all query instead of query_string
    if search_term == '*':
        # query['query']['match_all'] = {}
        del query['query']['query_string']
    # If searching for more than one type, don't specify which fields to search
    else:
        # del query['query']['bool']['must']['multi_match']['fields']
        query['query']['query_string']['fields'].extend(['_all', '*.uuid', '*.md5sum', '*.submitted_file_name'])


    # Set sort order
    set_sort_order(request, search_term, types, doc_types, query, result)

    # Setting filters
    used_filters = set_filters(request, query, result)

    # Adding facets to the query
    facets = [
        ('type', {'title': 'Data Type'}),
    ]
    if len(doc_types) == 1 and 'facets' in types[doc_types[0]].schema:
        facets.extend(types[doc_types[0]].schema['facets'].items())

    # Display all audits if logged in, or all but INTERNAL_ACTION if logged out
    for audit_facet in audit_facets:
        if search_audit and 'group.submitter' in principals or 'INTERNAL_ACTION' not in audit_facet[0]:
            facets.append(audit_facet)

    query['aggs'] = set_facets(facets, used_filters, principals, doc_types)

    # Search caching uses JSON as cache key, hence sorting the query
    query = sort_query(query)

    # Decide whether to use scan for results.
    do_scan = size is None or size > 1000

    # Send search request to proper indices
    if not request.params.get('type') or 'Item' in doc_types:
        es_index = RESOURCES_INDEX
    else:
        es_index = [types[type_name].item_type for type_name in doc_types if hasattr(types[type_name], 'item_type')]

    # Execute the query
    if do_scan:
        es_results = es.search(body=query, index=es_index, search_type='query_then_fetch')
    else:
        es_results = es.search(body=query, index=es_index, from_=from_, size=size)

    result['total'] = total = es_results['hits']['total']

    schemas = (types[item_type].schema for item_type in doc_types)
    result['facets'] = format_facets(
        es_results, facets, used_filters, schemas, total, principals)

    # Add batch actions
    result.update(search_result_actions(request, doc_types, es_results))

    # Add all link for collections
    if size is not None and size < result['total']:
        params = [(k, v) for k, v in request.params.items() if k != 'limit']
        params.append(('limit', 'all'))
        result['all'] = '%s?%s' % (request.resource_path(context), urlencode(params))

    if not result['total']:
        # http://googlewebmastercentral.blogspot.com/2014/02/faceted-navigation-best-and-5-of-worst.html
        request.response.status_code = 404
        result['notification'] = 'No results found'
        result['@graph'] = []
        return result if not return_generator else []

    result['notification'] = 'Success'
    # Format results for JSON-LD
    if not do_scan:
        graph = format_results(request, es_results['hits']['hits'], result)
        if return_generator:
            return graph
        else:
            result['@graph'] = list(graph)
            return result

    # Scan large result sets.
    del query['aggs']
    if size is None:
        # preserve_order=True has unexpected results in clustered environment
        # https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/helpers/__init__.py#L257
        hits = scan(es, query=query, index=es_index, preserve_order=False)
    else:
        hits = scan(es, query=query, index=es_index, from_=from_, size=size, preserve_order=False)
    graph = format_results(request, hits, result)

    # Support for request.embed() and `return_generator`
    if request.__parent__ is not None or return_generator:
        if return_generator:
            return graph
        else:
            result['@graph'] = list(graph)
            return result

    # Stream response using chunked encoding.
    # XXX BeforeRender event listeners not called.
    app_iter = iter_long_json('@graph', graph, result)
    request.response.content_type = 'application/json'
    if str is bytes:  # Python 2 vs 3 wsgi differences
        request.response.app_iter = app_iter  # Python 2
    else:
        request.response.app_iter = (s.encode('utf-8') for s in app_iter)
    return request.response


def iter_search_results(context, request):
    return search(context, request, return_generator=True)


@view_config(context=AbstractCollection, permission='list', request_method='GET',
             name='listing')
def collection_view_listing_es(context, request):
    # Switch to change summary page loading options
    if request.datastore != 'elasticsearch':
        return collection_view_listing_db(context, request)

    return search(context, request)


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
