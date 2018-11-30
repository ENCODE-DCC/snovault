"""
# Search View
Some Desc

## Inheritance
SearchView<-BaseView
### BaseView function dependencies
- _format_facets
"""
from urllib.parse import urlencode

from pyramid.httpexceptions import HTTPBadRequest  # pylint: disable=import-error

from elasticsearch.helpers import scan  # pylint: disable=import-error

from snovault.elasticsearch.interfaces import RESOURCES_INDEX
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
from snovault.viewconfigs.base_view import BaseView
import json
import logging
log = logging.getLogger(__name__)


class SearchView(BaseView):  # pylint: disable=too-few-public-methods
    '''Search View'''
    view_name = 'search'
    def __init__(
            self,
            context,
            request,
            search_type=None,
            return_generator=False,
            default_doc_types=None
        ):
        # pylint: disable=too-many-arguments
        super(SearchView, self).__init__(context, request)
        self._search_type = search_type
        self._return_generator = return_generator
        self._default_doc_types = default_doc_types or []
        self._context = context

    def preprocess_view(self, views=None, search_result_actions=None):  # pylint: disable=too-many-statements, too-many-branches, too-many-locals
        '''
        Main function to construct query and build view results json
        * Only publicly accessible function
        '''
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
        log.debug("search ES index: %s" % es_index)
        log.debug(json.dumps(query, indent=4, sort_keys=True))
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
                preserve_order=False
            )
        else:
            hits = scan(
                self._elastic_search,
                query=query,
                index=es_index,
                from_=from_,
                size=size,
                preserve_order=False
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
