from pyramid.security import effective_principals
from pyramid.httpexceptions import HTTPBadRequest
from snovault import TYPES
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from elasticsearch.helpers import scan
from urllib.parse import urlencode
from snovault.helpers.helper import (
    sort_query,
    get_filtered_query,
    set_sort_order,
    get_search_fields,
    list_visible_columns_for_schemas,
    list_result_fields,
    set_filters,
    set_facets,
    iter_long_json,
    format_results,
    get_pagination,
    prepare_search_term,
    normalize_query,
)


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
