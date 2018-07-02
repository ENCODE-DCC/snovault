from urllib.parse import urlencode
from pyramid.httpexceptions import HTTPBadRequest
from elasticsearch.helpers import scan
from snovault.viewconfigs.base_view import BaseView
from snovault.helpers.helper import (
    sort_query,
    get_filtered_query,
    set_sort_order,
    get_search_fields,
    list_visible_columns_for_schemas,
    list_result_fields,
    set_filters,
    set_facets,
    format_facets,
    iter_long_json,
    format_results,
    search_result_actions
)


DEFAULT_DOC_TYPES = [
    'AntibodyLot',
    'Award',
    'Biosample',
    'Dataset',
    'GeneticModification',
    'Page',
    'Pipeline',
    'Publication',
    'Software',
    'Target',
]


audit_facets = [
    ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
    ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
    ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
    ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
]


class SearchView(BaseView):
    def __init__(self, context, request, search_type=None, return_generator=False):
        super(SearchView, self).__init__(context, request)
        self.search_type = search_type
        self.return_generator = return_generator

    def construct_clear_filters(self):
        searchterm_specs = self.request.params.getall('searchTerm')
        searchterm_only = urlencode([("searchTerm", searchterm) for searchterm in searchterm_specs])
        if searchterm_only:
            # Search term in query string; clearing keeps that
            clear_qs = searchterm_only
        else:
            # Possibly type(s) in query string
            clear_qs = urlencode([("type", typ) for typ in self.doc_types])
        return self.request.route_path('search', slash='/') + (('?' + clear_qs) if clear_qs else '')

    def construct_filters(self):

        for item_type in self.doc_types:
            ti = self.types[item_type]
            qs = urlencode([
                (k.encode('utf-8'), v.encode('utf-8'))
                for k, v in self.request.params.items() if not (k == 'type' and self.types['Item' if v == '*' else v] is ti)
            ])
            filters = {
                'field': 'type',
                'term': ti.name,
                'remove': '{}?{}'.format(self.request.path, qs)
            }
        return filters

    def construct_doc_types(self, doc_types=None):
        if doc_types is not None:
            self.doc_types = doc_types
            return
        # Process doc_types
        if self.search_type is None:
            if '*' in self.doc_types:
                self.doc_types = ['Item']

        else:
            self.doc_types = [self.search_type]

        try:
            self.doc_types = sorted({self.types[name].name for name in self.doc_types})
        except KeyError:
            # Check for invalid types
            bad_types = [t for t in self.doc_types if t not in self.types]
            msg = "Invalid type: {}".format(', '.join(bad_types))
            raise HTTPBadRequest(explanation=msg)

        if not self.doc_types:
            # For form editing embedded searches
            if self.request.params.get('mode') == 'picker':
                self.doc_types = ['Item']
            # For /search/ with no type= use defalts
            else:
                self.doc_types = DEFAULT_DOC_TYPES

    def construct_result_views(self):

        # Add special views like Report and Matrix if search is a single type
        for item_type in self.doc_types:
            ti = self.types[item_type]

        if len(self.doc_types) == 1:
            views = []
            views.append({
                'href': self.request.route_path('report', slash='/') + self.search_base,
                'title': 'View tabular report',
                'icon': 'table',
            })
            # matrix is encoded in schema for type
            if hasattr(ti.factory, 'matrix'):
                views.append({
                    'href': self.request.route_path('matrix', slash='/') + self.search_base,
                    'title': 'View summary matrix',
                    'icon': 'th',
                })
            if hasattr(ti.factory, 'summary_data'):
                views.append({
                    'href': self.request.route_path('summary', slash='/') + self.search_base,
                    'title': 'View summary report',
                    'icon': 'summary',
                })

        return views

    def construct_query(self, static_items=None, sort=True):

        search_fields, highlights = get_search_fields(self.request,
                                                      self.doc_types)
        # Builds filtered query which supports multiple facet selection
        query = get_filtered_query(self.search_term,
                                   search_fields,
                                   sorted(list_result_fields(self.request,
                                                             self.doc_types)),
                                   self.principals,
                                   self.doc_types)
        # If no text search, use match_all query instead of query_string
        if self.search_term == '*':
            # query['query']['match_all'] = {}
            del query['query']['query_string']
        # If searching for more than one type, don't specify which fields to search
        else:
            # del query['query']['bool']['must']['multi_match']['fields']
            query['query']['query_string']['fields'].extend(['_all',
                                                             '*.uuid',
                                                             '*.md5sum',
                                                             '*.submitted_file_name'])
        # Set sort order
        set_sort_order(self.request,
                       self.search_term,
                       self.types,
                       self.doc_types,
                       query,
                       self.result)
        # Setting filters
        self.used_filters = set_filters(self.request,
                                        query,
                                        self.result,
                                        static_items)
        # set facets
        self.set_facets()

        query['aggs'] = set_facets(self.facets,
                                   self.used_filters,
                                   self.principals,
                                   self.doc_types)

        # Search caching uses JSON as cache key, hence sorting the query
        if sort:
            query = sort_query(query)

        return query

    def set_facets(self):
        if len(self.doc_types) == 1 and 'facets' in self.types[self.doc_types[0]].schema:
            self.facets.extend(self.types[self.doc_types[0]].schema['facets'].items())
        # Display all audits if logged in, or all but INTERNAL_ACTION if logged out
        for audit_facet in audit_facets:
            if self.search_audit and 'group.submitter' in self.principals or 'INTERNAL_ACTION' not in audit_facet[0]:
                self.facets.append(audit_facet)

    def construct_facets(self, es_results, total):
        # Update facets
        schemas = (self.types[item_type].schema for item_type in self.doc_types)
        facets = format_facets(es_results,
                               self.facets,
                               self.used_filters,
                               schemas,
                               total,
                               self.principals)
        return facets

    def query_elastic_search(self, query, es_index, from_=None, size=None):

        # Execute the query
        if size is None or size > 1000:
            es_results = self.elastic_search.search(body=query,
                                                    index=es_index,
                                                    search_type='query_then_fetch')
        else:
            es_results = self.elastic_search.search(body=query,
                                                    index=es_index,
                                                    from_=from_,
                                                    size=size,
                                                    request_cache=True)
        return es_results

    def get_total_results(self, es_results):
        total = es_results['hits']['total']
        if not total:
            # http://googlewebmastercentral.blogspot.com/2014/02/faceted-navigation-best-and-5-of-worst.html
            self.request.response.status_code = 404
            self.result['notification'] = 'No results found'
            self.result['@graph'] = []
        return total

    def preprocess_view(self):
        self.result['@id'] = '/search/' + self.search_base
        self.result['@type'] = ['Search']
        self.result['title'] = 'Search'
        self.result['clear_filters'] = self.construct_clear_filters()

        # Construct self.doc_types
        self.construct_doc_types()

        if self.doc_types != ['Item'] and self.doc_types != DEFAULT_DOC_TYPES:
            filters = self.construct_filters()
            self.result['filters'].append(filters)
            self.result['views'] = self.construct_result_views()

        #  Columns is used in report view
        schemas = [self.types[doc_type].schema for doc_type in self.doc_types]
        columns = list_visible_columns_for_schemas(self.request, schemas)
        # and here it is attached to the result for the UI
        if columns:
            self.result['columns'] = columns

        # Construct query
        query = self.construct_query()
        # When type is known, route search request to relevant index
        if self.request.params.get('type') or 'Item' in self.doc_types:
            self.es_index = [self.types[type_name].item_type for type_name in self.doc_types if hasattr(self.types[type_name], 'item_type')]

        # Query elastic search and update results
        es_results = self.query_elastic_search(query, self.es_index, self.from_, self.size)
        self.result['total'] = self.get_total_results(es_results)
        if not self.result['total']:
            return self.result if not self.return_generator else []

        self.result['notification'] = 'Success'

        # Update facets
        self.result['facets'] = self.construct_facets(es_results, self.result['total'])

        # Add batch actions
        self.result.update(search_result_actions(self.request, self.doc_types, es_results))

        # Add all link for collections
        if self.size is not None:
            if self.size < self.result['total']:
                params = [(k, v) for k, v in self.request.params.items() if k != 'limit']
                params.append(('limit', 'all'))
                self.result['all'] = '%s?%s' % (self.request.resource_path(self.context),
                                                urlencode(params))

            # Format results for JSON-LD
            if self.size <= 1000:
                graph = format_results(self.request,
                                       es_results['hits']['hits'],
                                       self.result)
                if self.return_generator:
                    return graph
                else:
                    self.result['@graph'] = list(graph)
                    return self.result
            else:
                del query['aggs']
                hits = scan(self.elastic_search,
                            query=query,
                            index=self.es_index,
                            from_=self.from_,
                            size=self.size,
                            preserve_order=False)
        else:
            # Scan large result sets.
            del query['aggs']
            # preserve_order=True has unexpected results in clustered environment
            # https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/helpers/__init__.py#L257
            hits = scan(self.elastic_search,
                        query=query,
                        index=self.es_index,
                        preserve_order=False)

        graph = format_results(self.request,
                               hits,
                               self.result)

        # Support for request.embed() and `return_generator`
        if self.request.__parent__ is not None:
            if self.return_generator:
                return graph
            else:
                self.result['@graph'] = list(graph)
                return self.result

        # Stream response using chunked encoding.
        # XXX BeforeRender event listeners not called.
        app_iter = iter_long_json('@graph', graph, self.result)
        self.request.response.content_type = 'application/json'
        if str is bytes:  # Python 2 vs 3 wsgi differences
            self.request.response.app_iter = app_iter  # Python 2
        else:
            self.request.response.app_iter = (s.encode('utf-8') for s in app_iter)
        return self.request.response
