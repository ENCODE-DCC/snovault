"""
# Base View
Some Desc
"""
from pyramid.security import effective_principals  # pylint: disable=import-error

from snovault import TYPES
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.helpers.helper import (
    get_pagination,
    prepare_search_term,
    normalize_query
)


class BaseView(object):  #pylint: disable=too-few-public-methods, too-many-instance-attributes
    '''Base View for all search based endpoints'''
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
    def _format_facets(
            es_results,
            facets,
            used_filters,
            schemas,
            total,
            principals
        ):
        '''Helper function for child classes'''
        # pylint: disable=too-many-locals, too-many-arguments
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
