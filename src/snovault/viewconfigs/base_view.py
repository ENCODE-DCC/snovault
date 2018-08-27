from pyramid.security import effective_principals
from snovault import TYPES
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.helpers.helper import (
    get_pagination,
    prepare_search_term,
    normalize_query
)


class BaseView(object):
    # Override it in child classes
    routename = None

    def __init__(self, context, request):
        self.request = request
        self.context = context
        self.types = self.request.registry[TYPES]
        self.search_base = normalize_query(self.request)
        self.result = {
            '@context': self.request.route_path('jsonld_context'),
            'filters': [],
        }
        self.doc_types = self.request.params.getall('type')
        self.principals = effective_principals(self.request)
        self.elastic_search = self.request.registry[ELASTIC_SEARCH]
        self.es_index = '_all'
        self.search_audit = self.request.has_permission('search_audit')
        self.search_term = prepare_search_term(self.request)
        self.request_cache = None
        self.facets = [
            ('type', {'title': 'Data Type'}),
        ]
        self.used_filters = None
        self.from_, self.size = get_pagination(self.request)
        self.result_list = {
            'href': self.request.route_path('search', slash='/') + self.search_base,
            'title': 'View results as list',
            'icon': 'list-alt',
        }
        self.tabular_report = {
            'href': self.request.route_path('report', slash='/') + self.search_base,
            'title': 'View tabular report',
            'icon': 'table',
        }
        self.summary_report = {
            'href': self.request.route_path('summary', slash='/') + self.search_base,
            'title': 'View summary report',
            'icon': 'summary',
        }
        self.summary_matrix = {
            'href': self.request.route_path('matrix', slash='/') + self.search_base,
            'title': 'View summary matrix',
            'icon': 'th',
        }
        
    @staticmethod
    def format_facets(es_results, facets, used_filters, schemas, total, principals):
        result = []
        # Loading facets in to the results
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
            result.append({
                'type': facet_type,
                'field': field,
                'title': options.get('title', field),
                'terms': terms,
                'total': all_buckets_total
            })
        # Show any filters that aren't facets as a fake facet with one entry,
        # so that the filter can be viewed and removed
        for field, values in used_filters.items():
            if field not in used_facets and field.rstrip('!') not in exists_facets:
                title = field
                for schema in schemas:
                    if field in schema['properties']:
                        title = schema['properties'][field].get('title', field)
                        break
                result.append({
                    'field': field,
                    'title': title,
                    'terms': [{'key': v} for v in values],
                    'total': total,
                })
        return result