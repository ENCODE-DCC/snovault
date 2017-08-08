import re
import math
from pyramid.view import view_config
from snovault import (
    AbstractCollection,
    TYPES,
)
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.resource_views import collection_view_listing_db
from snovault.fourfront_utils import get_jsonld_types_from_collection_type
from elasticsearch.helpers import scan
from elasticsearch_dsl import Search
from pyramid.httpexceptions import HTTPBadRequest
from pyramid.security import effective_principals
from urllib.parse import urlencode
from collections import OrderedDict
from copy import deepcopy


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('browse', '/browse{slash:/?}')
    config.add_route('available_facets', '/facets{slash:/?}')
    config.scan(__name__)

sanitize_search_string_re = re.compile(r'[\\\+\-\&\|\!\(\)\{\}\[\]\^\~\:\/\\\*\?]')

@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False, forced_type='Search'):
    """
    Search view connects to ElasticSearch and returns the results.
    """
    types = request.registry[TYPES]
    search_base = normalize_query(request, search_type)
    ### INITIALIZE RESULT.
    result = {
        '@context': request.route_path('jsonld_context'),
        '@id': '/' + forced_type.lower() + '/' + search_base,
        '@type': [forced_type],
        'title': forced_type,
        'filters': [],
        'facets': [],
        '@graph': [],
        'notification': '',
        'sort': {}
    }
    principals = effective_principals(request)

    es = request.registry[ELASTIC_SEARCH]
    search_audit = request.has_permission('search_audit')

    from_, size = get_pagination(request)

    before_date, after_date = get_date_range(request)

    # get desired frame for this search
    search_frame = request.params.get('frame', 'embedded')

    ### PREPARE SEARCH TERM
    prepared_terms = prepare_search_term(request)

    doc_types = set_doc_types(request, types, search_type)
    schemas = (types[item_type].schema for item_type in doc_types)

    # set ES index based on doc_type (one type per index)
    # if doc_type is item, search all indexes by setting es_index to None
    # If multiple, search all specified
    if 'Item' in doc_types:
        es_index = '_all'
    else:
        es_index = find_index_by_doc_types(request, doc_types, ['Item'])

    # establish elasticsearch_dsl class that will perform the search
    search = Search(using=es, index=es_index)

    # set up clear_filters path
    result['clear_filters'] = clear_filters_setup(request, doc_types, forced_type)

    ### SET TYPE FILTERS
    build_type_filters(result, request, doc_types, types)

    # get the fields that will be used as source for the search
    # currently, supports frame=raw/object but live faceting does not work
    # this is okay because the only non-embedded access will be programmatic
    source_fields = sorted(list_source_fields(request, doc_types, search_frame))

    ### GET FILTERED QUERY
    # Builds filtered query which supports multiple facet selection
    search, string_query = build_query(search, prepared_terms, source_fields)

    ### Set sort order
    search = set_sort_order(request, search, prepared_terms, types, doc_types, result)
    # TODO: implement BOOST here?

    ### Set filters
    search, query_filters, used_filters = set_filters(request, search, result, principals, doc_types, before_date, after_date)

    ### Set starting facets
    facets = initialize_facets(types, doc_types, search_audit, principals, prepared_terms, schemas)

    ### Adding facets to the query
    search = set_facets(search, facets, query_filters, string_query)

    ### Execute the query
    if size == 'all':
        es_results = get_all_results(search)
    elif size:
        offset_size = from_ + size
        size_search = search[from_:offset_size]
        es_results = execute_search(size_search)
    else:
        # fallback size for elasticsearch is 10
        es_results = execute_search(search)

    ### Record total number of hits
    result['total'] = total = es_results['hits']['total']
    result['facets'] = format_facets(es_results, facets, used_filters, schemas, total, search_frame)

    # Add batch actions
    # TODO: figure out exactly what this does. Provide download URLs?
    # Implement later
    # result.update(search_result_actions(request, doc_types, es_results))

    ### Add all link for collections
    if size not in (None, 'all') and size < result['total']:
        params = [(k, v) for k, v in request.params.items() if k != 'limit']
        params.append(('limit', 'all'))
        result['all'] = '%s?%s' % (request.resource_path(context), urlencode(params))

    if not result['total']:
        # http://googlewebmastercentral.blogspot.com/2014/02/faceted-navigation-best-and-5-of-worst.html
        request.response.status_code = 404
        result['notification'] = 'No results found'
        result['@graph'] = []
        return result if not return_generator else []

    columns = list_visible_columns_for_schemas(request, schemas)
    if columns:
        result['columns'] = columns

    result['notification'] = 'Success'

    ### Format results for JSON-LD
    graph = format_results(request, es_results['hits']['hits'], search_frame)

    if request.__parent__ is not None or return_generator:
        if return_generator:
            return graph
        else:
            result['@graph'] = list(graph)
            return result
    else:
        result['@graph'] = list(graph)
        return result


@view_config(route_name='browse', request_method='GET', permission='search')
def browse(context, request, search_type=None, return_generator=False):
    """
    Simply use search results for browse view
    """
    return search(context, request, search_type, return_generator, forced_type='Browse')


@view_config(context=AbstractCollection, permission='list', request_method='GET')
def collection_view(context, request):
    """
    Simply use search results for collections views (e.g./biosamples/)
    This is a redirect directly to the search page
    """
    return search(context, request, context.type_info.name, False, forced_type='Search')


@view_config(route_name='available_facets', request_method='GET', permission='search', renderer='json')
def get_available_facets(context, request, search_type=None):
    """
    Method to get available facets for a content/data type; built off of search() without querying Elasticsearch.
    Unlike in search(), due to lack of ES query, does not return possible terms nor counts of experiments matching the terms.
    """

    types = request.registry[TYPES]
    doc_types = set_doc_types(request, types, search_type)
    schemas = (types[item_type].schema for item_type in doc_types)
    principals = effective_principals(request)
    prepared_terms = prepare_search_term(request)
    facets = initialize_facets(types, doc_types, request.has_permission('search_audit'), principals, prepared_terms, schemas)

    ### Mini version of format_facets
    result = []
    for field, facet in facets:
        result.append({
            'field': field,
            'title': facet.get('title', field)
        })

    return result


def get_pagination(request):
    """
    Fill from_ and size parameters for search if given in the query string
    """
    from_ = request.params.get('from', 0)
    size = request.params.get('limit', 25)
    if size in ('all', ''):
       size = "all"
    else:
        try:
            size = int(size)
        except ValueError:
            size = 25
        try:
            from_ = int(from_)
        except ValueError:
            size = 0
    return from_, size


def get_date_range(request):
    """
    Get 'before' and 'after' params from the request.
    These determine the greater than and less than dates for the search
    Date format is yyyy-MM-dd HH:mm.
    """
    before = request.params.get('before', None)
    after = request.params.get('after', None)
    return before, after


def get_all_results(search):
    from_ = 0
    sizeIncrement = 1000 # Decrease this to like 5 or 10 to test.
    size = from_ + sizeIncrement

    first_search = search[from_:size] # get aggregations from here
    es_result = execute_search(first_search)

    total = es_result['hits'].get('total',0)
    extraRequestsNeeded = int(math.ceil(total / sizeIncrement)) - 1 # Decrease by 1 (first es_result already happened)

    if extraRequestsNeeded <= 0:
        return es_result

    while extraRequestsNeeded > 0:
        # print(str(extraRequestsNeeded) + " requests left to get all results.")
        from_ = from_ + sizeIncrement
        size = from_ + sizeIncrement
        subsequent_search = search[from_:size]
        subsequent_es_result = execute_search(subsequent_search)
        es_result['hits']['hits'] = es_result['hits']['hits'] + subsequent_es_result['hits'].get('hits', [])
        extraRequestsNeeded -= 1
        # print("Found " + str(len(es_result['hits']['hits'])) + ' results so far.')
    return es_result


def normalize_query(request, search_type):
    """
    Normalize the query used to make the search. If no type is provided,
    use type=Item
    """
    item_type = search_type if search_type != None else 'Item'
    types = request.registry[TYPES]
    fixed_types = (
        (k, types[v].name if k == 'type' and v in types else v)
        for k, v in request.params.items()
    )
    qs = urlencode([
        (k.encode('utf-8'), v.encode('utf-8'))
        for k, v in fixed_types
    ])
    # default to the search_type or type=Item if no type is specified
    qs = '?' + qs if qs else '?type=' + item_type
    if 'type=' not in qs:
        qs += ('&type=' + item_type)
    return qs


def clear_filters_setup(request, doc_types, forced_type):
    # Clear Filters path -- make a path that clears all non-datatype filters
    # and leaves in search query, if present
    seach_query_specs = request.params.getall('q')
    seach_query_url = urlencode([("q", seach_query) for seach_query in seach_query_specs])
    # types_url will always be present (always >=1 doc_type)
    types_url = urlencode([("type", typ) for typ in doc_types])
    if seach_query_url:
        clear_qs = seach_query_url + '&' + types_url
    else:
        # no search query provided
        clear_qs = types_url
    return request.route_path(forced_type.lower(), slash='/') + (('?' + clear_qs) if clear_qs else '')


def build_type_filters(result, request, doc_types, types):
    """
    Set the type filters for the search. If no doc_types, default to Item
    """
    if not doc_types:
        doc_types = ['Item']
    else:
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


def prepare_search_term(request):
    """
    Prepares search terms by making a dictionary where the keys are fields
    and the values are arrays of query strings
    Ignore certain keywords, such as type, format, and field
    """
    prepared_terms = {}
    prepared_vals = []
    for field, val in request.params.iteritems():
        if field.startswith('audit'):
            continue
        elif field == 'q': # searched string has field 'q'
            # people shouldn't provide multiple queries, but if they do,
            # combine them with AND logic
            if 'q' in prepared_terms:
                join_list = [prepared_terms['q'], val]
                prepared_terms['q'] = ' AND '.join(join_list)
            else:
                prepared_terms['q'] = val
        elif field not in ['type', 'frame', 'format', 'limit', 'sort', 'from', 'field', 'before', 'after']:
            if 'embedded.' + field not in prepared_terms.keys():
                prepared_terms['embedded.' + field] = []
            prepared_terms['embedded.' + field].append(val)
    if 'q' in prepared_terms:
        prepared_terms['q'] = process_query_string(prepared_terms['q'])
    return prepared_terms


def process_query_string(search_query):
    from antlr4 import IllegalStateException
    from lucenequery.prefixfields import prefixfields
    from lucenequery import dialects
    if search_query == '*':
        return search_query
    # avoid interpreting slashes as regular expressions
    search_query = search_query.replace('/', r'\/')
    try:
        query = prefixfields('embedded.', search_query, dialects.elasticsearch)
    except (IllegalStateException):
        msg = "Invalid query: {}".format(search_query)
        raise HTTPBadRequest(explanation=msg)
    else:
        return query.getText()


def set_doc_types(request, types, search_type):
    """
    Set the type of documents resulting from the search; order and check for
    invalid types as well.
    """
    doc_types = []
    if search_type is None:
        doc_types = request.params.getall('type')
        if '*' in doc_types:
            doc_types = ['Item']
    else:
        doc_types = [search_type]
    # Normalize to item_type
    try:
        doc_types = sorted({types[name].name for name in doc_types})
    except KeyError:
        # Check for invalid types
        bad_types = [t for t in doc_types if t not in types]
        msg = "Invalid type: {}".format(', '.join(bad_types))
        raise HTTPBadRequest(explanation=msg)
    if len(doc_types) == 0:
        doc_types = ['Item']
    return doc_types

def get_search_fields(request, doc_types):
    """
    Returns set of columns that are being searched and highlights
    """
    fields = {'uuid'}
    highlights = {}
    types = request.registry[TYPES]
    for doc_type in doc_types:
        type_info = types[doc_type]
        for value in type_info.schema.get('boost_values', ()):
            fields.add('embedded.' + value)
            highlights['embedded.' + value] = {}
    return fields, highlights


def list_source_fields(request, doc_types, frame):
    """
    Returns set of fields that are requested by user or default fields.
    These fields are used to further limit the results from the search.
    Note that you must provide the full fieldname with embeds, such as:
    'field=biosample.biosource.individual.organism.name' and not just
    'field=name'
    Add audit to this so we can look at that as well
    """
    fields_requested = request.params.getall('field')
    if fields_requested:
        fields = ['embedded.@id', 'embedded.@type']
        for field in fields_requested:
            fields.append('embedded.' + field)
    elif frame in ['embedded', 'object', 'raw']:
        if frame != 'embedded':
            # frame=raw corresponds to 'properties' in ES
            if frame == 'raw':
                frame = 'properties'
            # let embedded be searched as well (for faceting)
            fields = ['embedded.*', frame + '.*']
        else:
            fields = [frame + '.*']
    else:
        fields = ['embedded.*']
    return fields


def build_query(search, prepared_terms, source_fields):
    """
    Prepare the query within the Search object.
    """
    query_info = {}
    string_query = None
    # set _source fields for the search
    search = search.source(list(source_fields))
    # prepare the query from prepared_terms
    for field, value in prepared_terms.items():
        if field == 'q':
            query_info['query'] = value
            query_info['lenient'] = True
            break
    if query_info != {}:
        string_query = {'must': {'query_string': query_info}}
        query_dict = {'query': {'bool': string_query}}
    else:
        query_dict = {'query': {'bool':{}}}
    search.update_from_dict(query_dict)
    return search, string_query


def set_sort_order(request, search, search_term, types, doc_types, result):
    """
    sets sort order for elasticsearch results
    example: /search/?type=Biosource&sort_by=display_title
    will sort by display_title in ascending order. To set descending order,
    use the "-" flag: sort_by=-date_created.
    Sorting is done alphatbetically, case sensitive by default.
    TODO: add a schema flag for case sensitivity/insensitivity?

    ES5: simply pass in the sort OrderedDict into search.sort
    """
    sort = OrderedDict()
    result_sort = OrderedDict()
    # Prefer sort order specified in request, if any
    requested_sort = request.params.get('sort')
    if requested_sort:
        if requested_sort.startswith('-'):
            name = requested_sort[1:]
            order = 'desc'
        else:
            name = requested_sort
            order = 'asc'
        sort['embedded.' + name + '.lower_case_sort.keyword'] = result_sort[name] = {
            'order': order,
            'unmapped_type': 'keyword',
            'missing': '_last'
        }
    # Otherwise we use a default sort only when there's no text search to be ranked
    if not sort and search_term == '*':
        # If searching for a single type, look for sort options in its schema
        if len(doc_types) == 1:
            type_schema = types[doc_types[0]].schema
            if 'sort_by' in type_schema:
                for k, v in type_schema['sort_by'].items():
                    # Should always sort on raw field rather than analyzed field
                    # OR search on lower_case_sort for case insensitive results
                    sort['embedded.' + k + '.lower_case_sort.keyword'] = result_sort[k] = v
        # Default is most recent first, then alphabetical by label
        if not sort:
            sort['embedded.date_created.raw'] = result_sort['date_created'] = {
                'order': 'desc',
                'unmapped_type': 'keyword',
            }
            sort['embedded.label.raw'] = result_sort['label'] = {
                'order': 'asc',
                'missing': '_last',
                'unmapped_type': 'keyword',
            }
    if sort and result_sort:
        result['sort'] = result_sort
        search = search.sort(sort)
    return search


def set_filters(request, search, result, principals, doc_types, before_date=None, after_date=None):
    """
    Sets filters in the query
    """
    # initialize query_filters with filters for principals and doc_types
    query_filters = []
    not_query_filters = [] # filters used in NOT case
    query_filters.append({'terms': {'principals_allowed.view': principals}})
    query_filters.append({'terms': {'embedded.@type.raw': doc_types}})

    used_filters = {'must': {}, 'must_not': {}}
    for field, term in request.params.items():
        not_field = False # keep track if query is NOT (!)
        if field in ['limit', 'y.limit', 'x.limit', 'mode',
                     'format', 'frame', 'datastore', 'field', 'region', 'genome',
                     'sort', 'from', 'referrer', 'q', 'before', 'after']:
            continue
        elif field == 'type' and term != 'Item':
            continue
        # Add filter to result
        qs = urlencode([
            (k.encode('utf-8'), v.encode('utf-8'))
            for k, v in request.params.items() if v != term
        ])
        remove_path = '{}?{}'.format(request.path, qs)
        # default to searching type=Item rather than empty filter path
        if remove_path[-1] == '?':
            remove_path += 'type=Item'

        result['filters'].append({
            'field': field,
            'term': term,
            'remove': remove_path
        })

        if field == 'searchTerm':
            continue

        # handle NOT
        if field.endswith('!'):
            field = field[:-1]
            not_field = True

        # Add filter to query
        if field.startswith('audit'):
            query_field = field + '.raw'
        elif field == 'type':
            query_field = 'embedded.@type.raw'
        else:
            query_field = 'embedded.' + field + '.raw'

        bool_used_filters = used_filters['must_not'] if not_field else used_filters['must']
        if field not in bool_used_filters:
            bool_used_filters[field] = [term]
            this_filter = {'terms': {query_field: [term]}}
            if not_field:
                not_query_filters.append(this_filter)
            else:
                query_filters.append(this_filter)
        else:
            this_filter = {'terms': {query_field: bool_used_filters[field]}}
            if not_field:
                not_query_filters.remove(this_filter)
                # update this_filter to reflect used_filters change
                bool_used_filters[field].append(term)
                not_query_filters.append(this_filter)
            else:
                query_filters.remove(this_filter)
                # update this_filter to reflect used_filters change
                bool_used_filters[field].append(term)
                query_filters.append(this_filter)

    # lastly, add date limits to filters if given
    if before_date or after_date:
        date_limits = {'format':'yyyy-MM-dd HH:mm'}
        if before_date:
            date_limits['lte'] = before_date # lte is >=
        if after_date:
            date_limits['gte'] = after_date # gte is <=
        query_filters.append({'range':{'embedded.date_created': date_limits}})

    # To modify filters of elasticsearch_dsl Search, must call to_dict(),
    # modify that, then update from the new dict
    prev_search = search.to_dict()
    # initialize filter hierarchy
    final_filters = {'bool': {'must': query_filters, 'must_not': not_query_filters}}
    prev_search['query']['bool']['filter'] = final_filters
    search.update_from_dict(prev_search)

    return search, final_filters, used_filters


def initialize_facets(types, doc_types, search_audit, principals, prepared_terms, schemas):
    """
    Initialize the facets used for the search. If searching across multiple
    doc_types, only use the default 'Data Type' and 'Status' facets.
    Add facets for custom url filters whether or not they're in the schema
    """
    facets = [
        ('type', {'title': 'Data Type'})
    ]
    audit_facets = [
        ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
        ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
        ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
        ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
    ]
    if len(doc_types) == 1 and doc_types[0] != 'Item' and 'facets' in types[doc_types[0]].schema:
        facets.extend(types[doc_types[0]].schema['facets'].items())

    used_facets = [facet[0] for facet in facets]
    # add status to used_facets, which will be added to facets later
    used_facets.append('status')

    # add facets for any non-schema fields that are requested in the search
    for field in prepared_terms:
        if field.startswith('embedded'):
            split_field = field.strip().split('.')
            use_field = '.'.join(split_field[1:])
            # use the last part of the split field to get the title
            title_field = split_field[-1]
            if title_field in used_facets:
                continue
            for schema in schemas:
                if title_field in schema['properties']:
                    title_field = schema['properties'][title_field].get('title', title_field)
                    break
            facets.append((use_field, {'title': title_field}))

    # append status and audit facets automatically
    facets.append(('status', {'title': 'Status'}))
    for audit_facet in audit_facets:
        facets.append(audit_facet)

    return facets


def set_facets(search, facets, final_filters, string_query):
    """
    Sets facets in the query using filters
    ES5: simply sets aggs by calling update_from_dict after adding them in

    :param facets: A list of tuples containing (0) field in object dot notation,  and (1) a dict or OrderedDict with title property.
    :param final_filters: Dict of filters which are set for the ES query in set_filters
    :param string_query: Dict holding the query_string used in the search
    """
    aggs = {}
    facet_fields = dict(facets).keys() # List of first entry of tuples in facets list.
    # E.g. 'type','experimentset_type','experiments_in_set.award.project', ...

    for field in facet_fields:
        if field == 'type':
            query_field = 'embedded.@type.raw'
        elif field.startswith('audit'):
            query_field = field + '.raw'
        else:
            query_field = 'embedded.' + field + '.raw'

        agg_name = field.replace('.', '-')

        # default was size = 10, so only top 10 agg results were returned.
        # set size to 100.
        # https://github.com/10up/ElasticPress/wiki/Working-with-Aggregations
        aggregation = {
            'terms': {
                'size': 100,
                'field': query_field,
            }
        }

        facet_filters = deepcopy(final_filters['bool'])

        # Remove filters from fields they apply to.
        # For example, the 'biosource_type' aggs should not have any
        # biosource_type filter in place.
        # Handle 'must' and 'must_not' filters separately
        for filter_type in ['must', 'must_not']:
            if final_filters['bool'][filter_type] == []:
                continue
            for compare_filter in final_filters['bool'][filter_type]:
                # if not a terms filter, dont do anything (do use as a filter)
                if 'terms' not in compare_filter:
                    continue
                # there should only be one key here
                for compare_field in compare_filter['terms'].keys():
                    # remove filter for a given field for that facet
                    # skip this for type facet (field = 'type')
                    # since we always want to include that filter.
                    if compare_field == query_field and field != 'type':
                        facet_filters[filter_type].remove(compare_filter)

        # add the string_query, if present, to the bool term with facet_filters
        if string_query and string_query['must']:
            # combine statements within 'must' for each
            facet_filters['must'].append(string_query['must'])

        aggs[agg_name] = {
            'aggs': {
                agg_name : aggregation
            },
            'filter': {'bool': facet_filters},
        }
    # to achieve OR behavior within facets, search among GLOBAL results,
    # not just returned ones. to do this, wrap aggs in ['all_items']
    # and add "global": {} to top level aggs query
    # see elasticsearch global aggs for documentation (should be ES5 compliant)
    final_aggs = {
        'all_items': {
            'global': {},
            'aggs': aggs
        }
    }

    prev_search = search.to_dict()
    prev_search['aggs'] = final_aggs
    search.update_from_dict(prev_search)

    return search


def execute_search(search):
    """
    Use a general try-except here for now
    """
    try:
        es_results = search.execute().to_dict()
    except:
        raise HTTPBadRequest(explanation='Failed search query')
    return es_results


def format_facets(es_results, facets, used_filters, schemas, total, search_frame='embedded'):
    """
    Format the facets for the final results based on the es results
    These are stored within 'aggregations' of the result.

    If the frame for the search != embedded, return no facets
    """
    result = []
    if search_frame != 'embedded':
        return result

    # Loading facets in to the results
    if 'aggregations' not in es_results:
        return result

    aggregations = es_results['aggregations']['all_items']
    used_facets = set()
    for field, facet in facets:
        resultFacet = {
            'field' : field,
            'title' : facet.get('title', field),
            'total' : 0,
            'terms' : None
        }
        used_facets.add(field)
        agg_name = field.replace('.', '-')

        if agg_name in aggregations:
            resultFacet['total'] = aggregations[agg_name]['doc_count']
            resultFacet['terms'] = aggregations[agg_name][agg_name]['buckets']

        # Choosing to show facets with one term for summary info on search it provides
        if len(resultFacet.get('terms', [])) < 1:
            continue

        result.append(resultFacet)

    return result

def format_results(request, hits, search_frame):
    """
    Loads results to pass onto UI
    For now, add audits to the results so we can facet/not facet on audits
    """
    fields_requested = request.params.getall('field')
    if fields_requested:
        frame = 'embedded'
    elif search_frame:
        frame = search_frame
    else:
        frame = 'embedded'

    if frame in ['embedded', 'object', 'raw']:
        # transform 'raw' to 'properties', which is what is stored in ES
        if frame == 'raw':
            frame = 'properties'
        for hit in hits:
            frame_result = hit['_source'][frame]
            if 'audit' in hit['_source'] and 'audit' not in frame_result:
                frame_result['audit'] = hit['_source']['audit']
            yield frame_result
        return


def find_index_by_doc_types(request, doc_types, ignore):
    """
    Find the correct index(es) to be search given a list of doc_types.
    The types in doc_types are the collection names, formatted like
    'Experiment HiC' and index names are the jsonld_types, formatted like
    'experiment_hi_c'.
    Ignore any collection names provided in the ignore param, an array.
    Formats output indexes as a string usable by elasticsearch
    """
    indexes = []
    for doc_type in doc_types:
        if doc_type in ignore:
            continue
        else:
            result = get_jsonld_types_from_collection_type(request, doc_type)
            indexes.extend(result)
    # remove any duplicates
    indexes = list(set(indexes))
    index_string = ','.join(indexes)
    return index_string


### stupid things to remove; had to add because of other fxns importing

# Update? used in ./batch_download.py
def iter_search_results(context, request):
    return search(context, request, return_generator=True)

# DUMMY FUNCTION. TODO: update ./batch_download.py to use embeds instead of cols
def list_visible_columns_for_schemas(request, schemas):
    columns = OrderedDict()
    for schema in schemas:
        if 'columns' in schema:
            columns.update(OrderedDict(
                (name, obj.get('title'))
                for name,obj in schema['columns'].items() #if name in schema['properties']
            ))
    return columns

_ASSEMBLY_MAPPER = {
    'GRCh38-minimal': 'hg38',
    'GRCh38': 'hg38',
    'GRCh37': 'hg19',
    'GRCm38': 'mm10',
    'GRCm37': 'mm9',
    'BDGP6': 'dm4',
    'BDGP5': 'dm3',
    'WBcel235': 'WBcel235'
}

hgConnect = ''.join([
    'http://genome.ucsc.edu/cgi-bin/hgTracks',
    '?hubClear=',
])
