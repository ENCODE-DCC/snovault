"""
# Base View
Some Desc
"""
from pyramid.security import effective_principals  # pylint: disable=import-error
from pyramid.httpexceptions import HTTPBadRequest  # pylint: disable=import-error

from snovault import TYPES
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.helpers.helper import (
    get_pagination,
    prepare_search_term,
    normalize_query
)


class BaseView(object):  #pylint: disable=too-few-public-methods, too-many-instance-attributes
    """Base View for all search based endpoints."""

    _audit_facets = [
        ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
        ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
        ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
        ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
    ]

    def __init__(self, context, request):
        """
        Initialize.

            :param context: Pyramid context object
            :param request: Pyramid request object
        """
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
        self._es_index = '_all'
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
        self._view_name = None  # should be set by view
        self._factory_name = None  # view should set this

    def _type_info(self, doc_types, types):
        """
        Get type infrmation.

            :param doc_types: document types
            :param types: types
        """
        if doc_types and types and len(doc_types) == 1 and doc_types[0] in types:
            type_info = types[doc_types[0]]
            schema = type_info.schema
            return type_info, schema
        return None, None

    def _validate_items(self, type_info=None):
        """
        Ensure query string parameters are valid and throws a custom HTTP-500 error if not.

            :param type_info=None: type information
            :error: Custom HTTP-500 if query string parameters are invalid
        """
        msg = None
        if len(self._doc_types) != 1:
            msg = (
                'Search result {} currently requires specifying a '
                'single type.'.format(
                    self._view_name,
                )
            )
        elif self._doc_types[0] not in self._types:
            msg = 'Invalid type: {}'.format(self._doc_types[0])
        elif type_info and self._factory_name and not hasattr(type_info.factory, self._factory_name):
            msg = 'No {} configured for type: {}'.format(
                self._view_name,
                self._doc_types[0],
            )
        if msg:
            raise HTTPBadRequest(explanation=msg)

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
                    'total': all_buckets_total
                }
            )
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
