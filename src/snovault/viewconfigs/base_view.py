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
        self.audit_facets = ()