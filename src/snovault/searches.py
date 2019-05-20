from pyramid.security import effective_principals
from pyramid.httpexceptions import HTTPBadRequest
from snovault import TYPES
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from elasticsearch.helpers import scan
from urllib.parse import urlencode
import json
import re
from collections import OrderedDict
from urllib.parse import urlencode
from antlr4 import IllegalStateException
from lucenequery import dialects
from lucenequery.prefixfields import prefixfields


default_filter_exclusion = [
    'type', 'limit', 'mode', 'annotation', 'format', 'frame', 'datastore',
    'field', 'region', 'genome', 'sort', 'from', 'referrer', 'filterresponse',
    'remove', 'cart',
]

def sort_query(unsorted_query):
    sorted_query = OrderedDict()
    for field, value in sorted(unsorted_query.items()):
        if isinstance(value, dict):
            sorted_query[field] = sort_query(value)
        else:
            sorted_query[field] = value
    return sorted_query


def get_pagination(request):
    from_ = request.params.get('from') or 0
    size = request.params.get('limit', 25)
    if size in ('all', ''):
        size = None
    else:
        try:
            size = int(size)
        except ValueError:
            size = 25
    return from_, size


def get_filtered_query(term, search_fields, result_fields, principals, doc_types):
    return {
        'query': {
            'query_string': {
                'query': term,
                'fields': search_fields,
                'default_operator': 'AND'
            }
        },
        'post_filter': {
            'bool': {
                'must': [
                    {
                        'terms': {
                            'principals_allowed.view': principals
                        }
                    },
                    {
                        'terms': {
                            'embedded.@type': doc_types
                        }
                    }
                ],
                'must_not': []
            }
        },
        '_source': list(result_fields),
    }


def prepare_search_term(request):

    search_term = request.params.get('searchTerm', '').strip() or '*'
    if search_term == '*':
        return search_term

    # avoid interpreting slashes as regular expressions
    search_term = search_term.replace('/', r'\/').replace('^', r'').replace('<', r'').replace('>', r'')
    search_term = re.sub('\?+', '?', search_term)
    # elasticsearch uses : as field delimiter, but we use it as namespace designator
    # if you need to search fields you have to use @type:field
    # if you need to search fields where the field contains ":", you will have to escape it
    # yourself
    if search_term.find("@type") < 0:
        search_term = search_term.replace(':', r'\:')
    try:
        query = prefixfields('embedded.', search_term, dialects.elasticsearch)
    except IllegalStateException:
        msg = "Invalid query: {}".format(search_term)
        raise HTTPBadRequest(explanation=msg)
    else:
        return query.getText()


def set_sort_order(request, search_term, types, doc_types, query, result):
    """
    sets sort order for elasticsearch results
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
        # TODO: unmapped type needs to be determined, not hard coded
        sort['embedded.' + name] = result_sort[name] = {
            'order': order,
            'unmapped_type': 'keyword',
        }

    # Otherwise we use a default sort only when there's no text search to be  ranked
    if not sort and search_term == '*':

        # If searching for a single type, look for sort options in its schema
        if len(doc_types) == 1:
            type_schema = types[doc_types[0]].schema
            if 'sort_by' in type_schema:
                for key, value in type_schema['sort_by'].items():
                    # Should always sort on raw field rather than analyzed field
                    sort['embedded.' + key] = result_sort[key] = dict(value)

        # Default is most recent first, then alphabetical by label
        if not sort:
            sort['embedded.date_created'] = result_sort['date_created'] = {
                'order': 'desc',
                'unmapped_type': 'keyword',
            }
            sort['embedded.label'] = result_sort['label'] = {
                'order': 'asc',
                'missing': '_last',
                'unmapped_type': 'keyword',
            }

    if sort:
        query['sort'] = sort
        result['sort'] = result_sort
        return True

    return False


def get_search_fields(request, doc_types):
    """
    Returns set of columns that are being searched and highlights
    """

    fields = {'uuid', 'unique_keys.*'}
    highlights = {}
    types = request.registry[TYPES]
    for doc_type in doc_types:
        type_info = types[doc_type]
        for value in type_info.schema.get('boost_values', ()):
            fields.add('embedded.' + value)
            highlights['embedded.' + value] = {}
    return list(fields), highlights


def list_visible_columns_for_schemas(request, schemas):
    """
    Returns mapping of default columns for a set of schemas.
    """
    columns = OrderedDict({'@id': {'title': 'ID'}})
    for schema in schemas:
        if 'columns' in schema:
            columns.update(schema['columns'])
        else:
            # default columns if not explicitly specified
            columns.update(OrderedDict(
                (name, {
                    'title': schema['properties'][name].get('title', name)
                })
                for name in [
                    '@id', 'title', 'description', 'name', 'accession',
                    'aliases'
                ] if name in schema['properties']
            ))

    fields_requested = request.params.getall('field')
    if fields_requested:
        limited_columns = OrderedDict()
        for field in fields_requested:
            if field in columns:
                limited_columns[field] = columns[field]
            else:
                # We don't currently traverse to other schemas for embedded
                # objects to find property titles. In this case we'll just
                # show the field's dotted path for now.
                limited_columns[field] = {'title': field}
                for schema in schemas:
                    if field in schema['properties']:
                        limited_columns[field] = {
                            'title': schema['properties'][field]['title']
                        }
                        break
        columns = limited_columns

    return columns


def list_result_fields(request, doc_types):
    """
    Returns set of fields that are requested by user or default fields
    """
    frame = request.params.get('frame')
    fields_requested = request.params.getall('field')
    if fields_requested:
        fields = {'embedded.@id', 'embedded.@type'}
        fields.update('embedded.' + field for field in fields_requested)
    elif frame in ['embedded', 'object']:
        fields = [frame + '.*']
    else:
        frame = 'columns'
        # Fields that front-end expects is not returned as an empty array.
        # At this time, no way of knowing knowing which are those fields
        # that are not covered by tests, hence embedded.* for _source
        fields = {'embedded.@id', 'embedded.@type'}
        if request.has_permission('search_audit'):
            fields.add('audit.*')
        types = request.registry[TYPES]
        schemas = [types[doc_type].schema for doc_type in doc_types]
        columns = list_visible_columns_for_schemas(request, schemas)
        fields.update('embedded.' + column for column in columns)

    # Ensure that 'audit' field is requested with _source in the ES query
    if request.__parent__ and '/metadata/' in request.__parent__.url and request.has_permission('search_audit'):
        fields.add('audit.*')

    return fields


def build_terms_filter(query_filters, field, terms):
    if field.endswith('!'):
        field = field[:-1]
        if not field.startswith('audit'):
            field = 'embedded.' + field
        # Setting not filter instead of terms filter
        if terms == ['*']:
            negative_filter_condition = {
                'exists': {
                    'field': field,
                }
            }
        else:
            negative_filter_condition = {
                'terms': {
                    field: terms
                }
            }
        query_filters['must_not'].append(negative_filter_condition)
    else:
        if not field.startswith('audit'):
            field = 'embedded.' + field
        if terms == ['*']:
            filter_condition = {
                'exists': {
                    'field': field,
                }
            }
        else:
            filter_condition = {
                'terms': {
                    field: terms,
                },
            }
        query_filters['must'].append(filter_condition)


def set_filters(request, query, result, static_items=None, filter_exclusion=None):
    """
    Sets filters in the query
    """
    query_filters = query['post_filter']['bool']
    used_filters = {}
    if static_items is None:
        static_items = []

    # Get query string items plus any static items, then extract all the fields
    qs_items = list(request.params.items())
    total_items = qs_items + static_items
    qs_fields = [item[0] for item in qs_items]
    fields = [item[0] for item in total_items]

    # Now make lists of terms indexed by field
    all_terms = {}
    for item in total_items:
        if item[0] in all_terms:
            all_terms[item[0]].append(item[1])
        else:
            all_terms[item[0]] = [item[1]]

    for field in fields:
        if field in used_filters:
            continue

        terms = all_terms[field]
        if field in (filter_exclusion or default_filter_exclusion):
            continue

        # Add filter to result
        filterresponse = request.params.get('filterresponse', 'on')
        if field in qs_fields and filterresponse == 'on':
            for term in terms:
                query_string = urlencode([
                    (k.encode('utf-8'), v.encode('utf-8'))
                    for k, v in qs_items
                    if '{}={}'.format(k, v) != '{}={}'.format(field, term)
                ])
                result['filters'].append({
                    'field': field,
                    'term': term,
                    'remove': '{}?{}'.format(request.path, query_string)
                })

        if field == 'searchTerm':
            continue

        # Add to list of active filters
        used_filters[field] = terms

        # Add filter to query
        build_terms_filter(query_filters, field, terms)

    return used_filters


def build_aggregation(facet_name, facet_options, min_doc_count=0):
    """Specify an elasticsearch aggregation from schema facet configuration.
    """
    exclude = []
    if facet_name == 'type':
        field = 'embedded.@type'
        exclude = ['Item']
    elif facet_name.startswith('audit'):
        field = facet_name
    else:
        field = 'embedded.' + facet_name
    agg_name = facet_name.replace('.', '-')

    facet_type = facet_options.get('type', 'terms')
    facet_length = 200
    if facet_options.get('length') == 'long':
        facet_length = 3000
    if facet_type in ['terms', 'typeahead']:
        agg = {
            'terms': {
                'field': field,
                'min_doc_count': min_doc_count,
                'size': facet_length,
            },
        }
        if exclude:
            agg['terms']['exclude'] = exclude
    elif facet_type == 'exists':
        agg = {
            'filters': {
                'filters': {
                    'yes': {
                        'bool': {
                            'must': {
                                'exists': {'field': field}
                            }
                        }
                    },
                    'no': {
                        'bool': {
                            'must_not': {
                                'exists': {'field': field}
                            }
                        }
                    },
                },
            },
        }
    else:
        raise ValueError('Unrecognized facet type {} for {} facet'.format(
            facet_type, field))

    return agg_name, agg


def set_facets(facets, used_filters, principals, doc_types):
    """
    Sets facets in the query using filters
    """
    aggs = {}
    for facet_name, facet_options in facets:
        # Filter facet results to only include
        # objects of the specified type(s) that the user can see
        filters = [
            {'terms': {'principals_allowed.view': principals}},
            {'terms': {'embedded.@type': doc_types}},
        ]
        negative_filters = []
        # Also apply any filters NOT from the same field as the facet
        for field, terms in used_filters.items():
            if field.endswith('!'):
                query_field = field[:-1]
            else:
                query_field = field

            # if an option was selected in this facet,
            # don't filter the facet to only include that option
            if query_field == facet_name:
                continue

            if not query_field.startswith('audit'):
                query_field = 'embedded.' + query_field

            if field.endswith('!'):
                if terms == ['*']:
                    negative_filters.append({'exists': {'field': query_field}})
                else:
                    negative_filters.append({'terms': {query_field: terms}})
            else:
                if terms == ['*']:
                    filters.append({'exists': {'field': query_field}})
                else:
                    filters.append({'terms': {query_field: terms}})

        agg_name, agg = build_aggregation(facet_name, facet_options)
        aggs[agg_name] = {
            'aggs': {
                agg_name: agg
            },
            'filter': {
                'bool': {
                    'must': filters,
                    'must_not': negative_filters
                },
            },
        }

    return aggs

def format_results(request, hits, result=None):
    """
    Loads results to pass onto UI
    """
    fields_requested = request.params.getall('field')
    if fields_requested:
        frame = 'embedded'
    else:
        frame = request.params.get('frame')

    # Request originating from metadata generation will skip to
    # partion of the code that adds audit  object to result items
    if request.__parent__ and '/metadata/' in request.__parent__.url:
        frame = ''

    any_released = False  # While formatting, figure out if any are released.

    if frame in ['embedded', 'object']:
        for hit in hits:
            if not any_released and hit['_source'][frame].get('status', 'released') == 'released':
                any_released = True
            yield hit['_source'][frame]
    else:
        # columns
        for hit in hits:
            item = hit['_source']['embedded']
            if not any_released and item.get('status', 'released') == 'released':
                any_released = True # Not exp? 'released' to do the least harm
            if 'audit' in hit['_source']:
                item['audit'] = hit['_source']['audit']
            if 'highlight' in hit:
                item['highlight'] = {}
                for key in hit['highlight']:
                    item['highlight'][key[9:]] = list(set(hit['highlight'][key]))
            yield item

    # After all are yielded, it may not be too late to change this result setting
    #if not any_released and result is not None and 'batch_hub' in result:
    #    del result['batch_hub']
    if not any_released and result is not None and 'visualize_batch' in result:
        del result['visualize_batch']

def normalize_query(request):
    types = request.registry[TYPES]
    fixed_types = (
        (k, types[v].name if k == 'type' and v in types else v)
        for k, v in request.params.items()
    )
    query_string = urlencode([
        (k.encode('utf-8'), v.encode('utf-8'))
        for k, v in fixed_types
    ])
    return '?' + query_string if query_string else ''


def iter_long_json(name, iterable, other):

    start = None

    # Note: by yielding @graph (iterable) first, then the
    # contents of result (other) *may* be altered based upon @graph
    iter_ = iter(iterable)
    try:
        first = next(iter_)
    except StopIteration:
        pass
    else:
        # yield json.dumps(first)
        start = '{' + json.dumps(name) + ':['
        yield start + json.dumps(first)
        for value in iter_:
            yield ',' + json.dumps(value)

    if start is None:  # Nothing has bee yielded yet
        yield json.dumps(other)
    else:
        other_stuff = (',' + json.dumps(other)[1:-1]) if other else ''
        yield ']' + other_stuff + '}'


class Base:

    _audit_facets = [
        ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
        ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
        ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
        ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
    ]

    def __init__(self, context, request):
        self._request = request
        self._context = context
        self._types = request.registry[TYPES]
        self._search_base = normalize_query(request)
        self._result = {
            '@context': request.route_path('jsonld_context'),
            'filters': [],
        }
        self._doc_types = request.params.getall('type')
        self._principals = effective_principals(request)
        self._elastic_search = request.registry[ELASTIC_SEARCH]
        self._es_index = RESOURCES_INDEX
        self._search_audit = request.has_permission('search_audit')
        self._search_term = prepare_search_term(request)
        self._request_cache = None
        self._facets = [
            ('type', {'title': 'Data Type'}),
        ]
        self.from_, self.size = get_pagination(request)
        from_, page_size = get_pagination(request)
        self._from_ = from_
        self._size = page_size

    @staticmethod
    def _format_facets(es_results, facets, used_filters, schemas, total, principals):
        result = []
        if 'aggregations' not in es_results:
            return result
        aggregations = es_results['aggregations']
        used_facets = set()
        exists_facets = set()
        for field, options in facets:
            used_facets.add(field)
            agg_name = field.replace('.', '-')
            if agg_name not in aggregations:
                continue
            all_buckets_total = aggregations[agg_name]['doc_count']
            if not all_buckets_total > 0:
                continue
            # internal_status exception. Only display for admin users
            if field == 'internal_status' and 'group.admin' not in principals:
                continue
            facet_type = options.get('type', 'terms')
            terms = aggregations[agg_name][agg_name]['buckets']
            if facet_type == 'exists':
                terms = [
                    {'key': 'yes', 'doc_count': terms['yes']['doc_count']},
                    {'key': 'no', 'doc_count': terms['no']['doc_count']},
                ]
                exists_facets.add(field)
            result.append(
                {
                    'type': facet_type,
                    'field': field,
                    'title': options.get('title', field),
                    'terms': terms,
                    'appended': 'false',
                    'total': all_buckets_total
                }
            )
        for field, values in used_filters.items():
            field_without_bang = field.rstrip('!')
            if field_without_bang not in used_facets and field_without_bang not in exists_facets:
                title = field_without_bang
                for schema in schemas:
                    if field in schema['properties']:
                        title = schema['properties'][field].get('title', field)
                        break
                item = [r for r in result if r['field'] == field_without_bang]
                terms = [{'key': v, 'isEqual': 'true' if field[-1] != '!' else 'false'} for v in values]
                if item:
                    item[0]['terms'].extend(terms)
                else:
                    result.append({
                        'field': field_without_bang,
                        'title': title,
                        'terms': terms,
                        'appended': 'true',
                        'total': total,
                    })
        return result


class Search(Base):
    view_name = 'search'
    def __init__(self, context, request, search_type=None, return_generator=False, default_doc_types=None):
        super().__init__(context, request)
        self._search_type = search_type
        self._return_generator = return_generator
        self._default_doc_types = default_doc_types or []
        self._context = context

    def preprocess_view(self, views=None, search_result_actions=None, preserve_order=False):
        types = self._types
        search_base = normalize_query(self._request)
        result = {
            '@context': self._request.route_path('jsonld_context'),
            '@id': '/search/' + search_base,
            '@type': ['Search'],
            'title': 'Search',
            'filters': [],
        }
        es_index = RESOURCES_INDEX
        search_audit = self._request.has_permission('search_audit')
        from_, size = get_pagination(self._request)
        search_term = prepare_search_term(self._request)
        if (
                hasattr(self._context, 'type_info') and
                hasattr(self._context.type_info, 'name') and
                self._context.type_info.name
        ):
            doc_types = [self._context.type_info.name]
        else:
            doc_types = self._request.params.getall('type')
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
        searchterm_specs = self._request.params.getall('searchTerm')
        searchterm_only = urlencode(
            [
                ("searchTerm", searchterm)
                for searchterm in searchterm_specs
            ]
        )
        if searchterm_only:
            clear_qs = searchterm_only
        else:
            clear_qs = urlencode([("type", typ) for typ in doc_types])
        search_route = self._request.route_path('search', slash='/')
        clear_route = '?' + clear_qs if clear_qs else ''
        result['clear_filters'] = search_route + clear_route
        if not doc_types:
            if self._request.params.get('mode') == 'picker':
                doc_types = ['Item']
            else:
                doc_types = self._default_doc_types
        else:
            for item_type in doc_types:
                t_thing = types[item_type]
                q_thing = urlencode(
                    [
                        (k.encode('utf-8'), v.encode('utf-8'))
                        for k, v in self._request.params.items()
                        if not (k == 'type' and types['Item' if v == '*' else v] is t_thing)
                    ]
                )
                result['filters'].append({
                    'field': 'type',
                    'term': t_thing.name,
                    'remove': '{}?{}'.format(self._request.path, q_thing)
                })
            if views:
                result['views'] = views
        search_fields, _ = get_search_fields(self._request, doc_types)
        query = get_filtered_query(
            search_term,
            search_fields,
            sorted(list_result_fields(self._request, doc_types)),
            self._principals,
            doc_types,
        )
        schemas = [types[doc_type].schema for doc_type in doc_types]
        columns = list_visible_columns_for_schemas(self._request, schemas)
        if columns:
            result['columns'] = columns
        if search_term == '*':
            del query['query']['query_string']
        else:
            query['query']['query_string']['fields'].extend(
                ['_all', '*.uuid', '*.md5sum', '*.submitted_file_name']
            )
        set_sort_order(self._request, search_term, types, doc_types, query, result)
        used_filters = set_filters(self._request, query, result)
        facets = [
            ('type', {'title': 'Data Type'}),
        ]
        if len(doc_types) == 1 and 'facets' in types[doc_types[0]].schema:
            facets.extend(types[doc_types[0]].schema['facets'].items())
        for audit_facet in self._audit_facets:
            if (
                    search_audit and
                    'group.submitter' in self._principals or
                    'INTERNAL_ACTION' not in audit_facet[0]
                ):
                facets.append(audit_facet)
        query['aggs'] = set_facets(facets, used_filters, self._principals, doc_types)
        query = sort_query(query)
        do_scan = size is None or size > 1000
        if not self._request.params.get('type') or 'Item' in doc_types:
            es_index = RESOURCES_INDEX
        else:
            es_index = [
                types[type_name].item_type
                for type_name in doc_types
                if hasattr(types[type_name], 'item_type')
            ]
        if do_scan:
            es_results = self._elastic_search.search(
                body=query,
                index=es_index,
                search_type='query_then_fetch'
            )
        else:
            es_results = self._elastic_search.search(
                body=query,
                index=es_index,
                from_=from_, size=size,
                request_cache=True
            )
        total = es_results['hits']['total']
        result['total'] = total
        schemas = (types[item_type].schema for item_type in doc_types)
        result['facets'] = self._format_facets(
            es_results,
            facets,
            used_filters,
            schemas,
            total,
            self._principals
        )
        if search_result_actions:
            result.update(
                search_result_actions(
                    self._request, doc_types, es_results
                )
            )
        if size is not None and size < result['total']:
            params = [(k, v) for k, v in self._request.params.items() if k != 'limit']
            params.append(('limit', 'all'))
            result['all'] = '%s?%s' % (
                self._request.resource_path(self._context),
                urlencode(params)
            )
        if not result['total']:
            self._request.response.status_code = 404
            result['notification'] = 'No results found'
            result['@graph'] = []
            return result if not self._return_generator else []
        result['notification'] = 'Success'
        if not do_scan:
            graph = format_results(
                self._request,
                es_results['hits']['hits'],
                result
            )
            if self._return_generator:
                return graph
            result['@graph'] = list(graph)
            return result
        del query['aggs']
        if size is None:
            hits = scan(
                self._elastic_search,
                query=query,
                index=es_index,
                preserve_order=preserve_order
            )
        else:
            hits = scan(
                self._elastic_search,
                query=query,
                index=es_index,
                from_=from_,
                size=size,
                preserve_order=preserve_order
            )
        graph = format_results(self._request, hits, result)
        if self._request.__parent__ is not None or self._return_generator:
            if self._return_generator:
                return graph
            result['@graph'] = list(graph)
            return result
        app_iter = iter_long_json('@graph', graph, result)
        self._request.response.content_type = 'application/json'
        if str is bytes:  # Python 2 vs 3 wsgi differences
            self._request.response.app_iter = app_iter  # Python 2
        else:
            self._request.response.app_iter = (
                item.encode('utf-8') for item in app_iter
            )
        return self._request.response


class Report(Search):
    view_name = 'report'
    _factory_name = None

    def __init__(self, context, request):
        super().__init__(context, request)

    def preprocess_view(self, views=None, search_result_actions=None):
        '''
        Main function to construct query and build view results json
        * Only publicly accessible function
        '''
        doc_types = self._request.params.getall('type')
        if len(doc_types) != 1:
            msg = 'Report view requires specifying a single type.'
            raise HTTPBadRequest(explanation=msg)
        try:
            sub_types = self._types[doc_types[0]].subtypes
        except KeyError:
            msg = "Invalid type: " + doc_types[0]
            raise HTTPBadRequest(explanation=msg)
        if len(sub_types) > 1:
            msg = 'Report view requires a type with no child types.'
            raise HTTPBadRequest(explanation=msg)
        _, size = get_pagination(self._request)
        if ('limit' in self._request.GET and self._request.__parent__ is None
                and (size is None or size > 1000)):
            del self._request.GET['limit']
        # TODO: Fix creating a new instance a SearchView
        # We already do this in __init__
        res = Search(self._context, self._request).preprocess_view(
            views=views,
            search_result_actions=search_result_actions,
            preserve_order=True,
        )
        view = {
            'href': res['@id'],
            'title': 'View results as list',
            'icon': 'list-alt',
        }
        if not res.get('views'):
            res['views'] = [view]
        else:
            res['views'][0] = view
        search_base = normalize_query(self._request)
        res['@id'] = '/report/' + search_base
        res['title'] = 'Report'
        res['@type'] = ['Report']
        res['non_sortable'] = TEXT_FIELDS
        return res
