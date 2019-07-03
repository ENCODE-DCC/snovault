from elasticsearch_dsl import A
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search
from itertools import chain
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.interfaces import TYPES

from .defaults import BASE_AUDIT_FACETS
from .defaults import BASE_FIELD_FACETS
from .defaults import BASE_SEARCH_FIELDS
from .defaults import INTERNAL_AUDIT_FACETS
from .defaults import NOT_FILTERS
from .interfaces import AND
from .interfaces import AND_JOIN
from .interfaces import AND_NOT_JOIN
from .interfaces import AUDIT
from .interfaces import BOOL
from .interfaces import BOOST_VALUES
from .interfaces import DASH
from .interfaces import EMBEDDED
from .interfaces import EMBEDDED_TYPE
from .interfaces import EXCLUDE
from .interfaces import EXISTS
from .interfaces import FACETS
from .interfaces import FILTERS
from .interfaces import GROUP_SUBMITTER
from .interfaces import ITEM
from .interfaces import LENGTH
from .interfaces import LONG
from .interfaces import NOT_JOIN
from .interfaces import NO
from .interfaces import PERIOD
from .interfaces import PICKER
from .interfaces import PRINCIPALS_ALLOWED_VIEW
from .interfaces import QUERY_STRING
from .interfaces import SEARCH_AUDIT
from .interfaces import _SOURCE
from .interfaces import TITLE
from .interfaces import TERMS
from .interfaces import TYPE_KEY
from .interfaces import WILDCARD
from .interfaces import YES


class AbstractQueryFactory():
    '''
    Interface for building specific queries. Don't change functionality here, instead
    inherit and extend/override functions as needed.
    '''

    def __init__(self, params_parser, *args, **kwargs):
        self.search = None
        self.params_parser = params_parser
        self.args = args
        self.kwargs = kwargs

    def _get_or_create_search(self):
        if self.search is None:
            self.search = Search(
                using=self._get_client(),
                index=self._get_index(),
            )
        return self.search

    def _get_client(self):
        return self.kwargs.get('client') or self.params_parser._request.registry[ELASTIC_SEARCH]

    def _get_index(self):
        return RESOURCES_INDEX

    def _get_principals(self):
        return self.params_parser._request.effective_principals

    def _get_schema_for_item_type(self, item_type):
        return self.params_parser._request.registry[TYPES][item_type].schema

    def _get_boost_values_for_item_type(self, item_type):
        return self._get_schema_for_item_type(item_type).get(
            BOOST_VALUES,
            {}
        )

    def _get_facets_for_item_type(self, item_type):
        return self._get_schema_for_item_type(item_type).get(FACETS, {}).items()

    def _show_internal_audits(self):
        if all([
                self.params_parser._request.has_permission(SEARCH_AUDIT),
                GROUP_SUBMITTER in self._get_principals()
        ]):
            return True
        return False

    def _get_audit_facets(self):
        if self._show_internal_audits():
            return BASE_AUDIT_FACETS + INTERNAL_AUDIT_FACETS
        return BASE_AUDIT_FACETS

    def _get_item_types(self):
        return self.params_parser.get_type_filters()

    def _get_default_item_types(self):
        mode = self.params_parser.get_one_value(
            params=self._get_mode()
        )
        if mode == PICKER:
            return [ITEM]
        return self.kwargs.get('default_item_types', [])

    def _get_default_facets(self):
        return self.kwargs.get(
            'default_facets',
            BASE_FIELD_FACETS + self._get_audit_facets()
        )

    def _get_default_and_maybe_item_facets(self):
        facets = self._get_default_facets()
        item_type_values = self.params_parser.param_values_to_list(
            params=self._get_item_types()
        )
        if len(item_type_values) == 1:
            facets.extend(
                self._get_facets_for_item_type(
                    item_type_values[0]
                )
            )
        return facets

    def _get_query(self):
        return self._combine_search_term_queries(
            must_match_filters=self.params_parser.get_must_match_search_term_filters(),
            must_not_match_filters=self.params_parser.get_must_not_match_search_term_filters()
        )

    def _get_filters(self):
        return self.params_parser.get_not_keys_filters(not_keys=NOT_FILTERS)

    def _get_post_filters(self):
        return self.kwargs.get('post_filters', self._get_filters() + self._get_item_types())
        
    def _get_sort(self):
        return self.params_parser.get_sort()

    def _get_limit(self):
        return self.params_parser.get_limit()

    def _get_mode(self):
        return self.params_parser.get_mode()

    def _get_search_fields(self):
        search_fields = set()
        search_fields.update(BASE_SEARCH_FIELDS)
        item_types = (
            self.params_parser.param_values_to_list(self._get_item_types())
            or self._get_default_item_types()
        )
        for item_type in item_types:
            search_fields.update(
                self._prefix_values(
                    EMBEDDED,
                    self._get_boost_values_for_item_type(item_type).keys()
                )
            )
        return list(search_fields)

    def _get_return_fields(self):
        return [EMBEDDED + WILDCARD]

    def _get_facets(self):
        return self.kwargs.get('facets', self._get_default_and_maybe_item_facets())

    def _get_facet_size(self):
        return self.kwargs.get('facet_size')

    def _prefix_value(self, prefix, value):
        return prefix + value

    def _prefix_values(self, prefix, values):
        return [
            self._prefix_value(prefix, v)
            for v in values
        ]

    def _combine_search_term_queries(self, must_match_filters=[], must_not_match_filters=[]):
        must = AND_JOIN.join(['({})'.format(q[1]) for q in must_match_filters])
        must_not = AND_NOT_JOIN.join(['({})'.format(q[1]) for q in must_not_match_filters])
        if must and must_not:
            return must + AND_NOT_JOIN + must_not
        elif must:
            return must
        elif must_not:
            return NOT_JOIN.lstrip() + must_not

    def _make_query_string_query(self, query, fields, default_operator=AND):
        return Q(
            QUERY_STRING,
            query=query,
            fields=fields,
            default_operator=default_operator
        )

    def _make_bool_query(self, **kwargs):
        return Q(
            BOOL,
            **kwargs
        )

    def _make_queries_from_params(self, query_context, params):
        return [
            query_context(
                field=self._map_param_key_to_elasticsearch_field(param_key=field),
                terms=terms
            )
            for field, terms in self.params_parser.group_values_by_key(
                    self.params_parser.remove_not_flag(
                        params=params
                    )
            ).items()
        ]
        

    def _make_must_equal_terms_query(self, field, terms, **kwargs):
        return Q(
            TERMS,
            **{field: terms}
        )

    def _make_must_equal_terms_queries_from_params(self, params):
        return self._make_queries_from_params(
            query_context=self._make_must_equal_terms_query,
            params=params
        )

    def _make_field_must_exist_query(self, field, **kwargs):
        return Q(
            EXISTS,
            field=field
        )

    def _make_field_must_exist_query_from_params(self, params):
        return self._make_queries_from_params(
            query_context=self._make_field_must_exist_query,
            params=params
        )

    def _make_default_filters(self):
        return [
            self._make_must_equal_terms_query(
                field=PRINCIPALS_ALLOWED_VIEW,
                terms=self._get_principals()
            ),
            self._make_must_equal_terms_query(
                field=EMBEDDED_TYPE,
                terms=(
                    self.params_parser.param_values_to_list(
                        params=self.params_parser.get_must_match_filters(
                            params=self._get_item_types()
                        )
                    )
                    or self._get_default_item_types()
                )
            )
        ]

    def _make_split_filter_queries(self, params=None):
        '''
        Returns appropriate queries from param filters.
        '''
        _must, _must_not, _exists, _not_exists = self.params_parser.split_filters_by_must_and_exists(
            params=params or self._get_post_filters()
        )
        must = self._make_must_equal_terms_queries_from_params(_must)
        must_not = self._make_must_equal_terms_queries_from_params(_must_not)
        exists = self._make_field_must_exist_query_from_params(_exists)
        not_exists = self._make_field_must_exist_query_from_params(_not_exists)
        return must, must_not, exists, not_exists

    def _make_terms_aggregation(self, field, exclude=[], size=200, *kwargs):
        return A(
            TERMS,
            field=field,
            size=size,
            exclude=exclude
        )

    def _make_exists_aggregation(self, field):
        return A(
            FILTERS,
            filters={
                YES: Q(EXISTS, field=field),
                NO: ~Q(EXISTS, field=field)
            }
        )

    def _make_filter_aggregation(self, filter_context, **kwargs):
        return A(
            'filter',
            filter_context
        )

    def _make_filter_and_subaggregation(self, title, filter_context, subaggregation):
        a = self._make_filter_aggregation(filter_context)
        a.bucket(title, subaggregation)
        return a

    def _map_param_key_to_elasticsearch_field(self, param_key):
        '''
        Special rules for mapping param key to actual field in ES.
        For exampe type -> embedded.@type.
        '''

        if param_key == TYPE_KEY:
            return EMBEDDED_TYPE
        elif param_key.startswith(AUDIT):
            return param_key
        else:
            return self._prefix_value(EMBEDDED, param_key)

    def _map_params_to_elasticsearch_fields(self, params):
        '''
        Like _map_param_key_to_elasticsearch_field but used for iterating over list
        of param tuples.
        '''
        for param_key, param_value in params:
            yield (
                self._map_param_key_to_elasticsearch_field(param_key),
                param_value
            )

    def _subaggregation_factory(self, facet_option_type):
        if facet_option_type == EXISTS:
            return self._make_exists_aggregation
        return self._make_terms_aggregation

    def _add_must_equal_terms_filter(self, field, terms):
        self.search = self._get_or_create_search().filter(
            self._make_must_equal_terms_query(
                field=field,
                terms=terms
            )
        )

    def _add_must_equal_terms_post_filter(self, field, terms):
        self.search = self._get_or_create_search().post_filter(
            self._make_must_equal_terms_query(
                field=field,
                terms=terms
            )
        )

    def _add_must_not_equal_terms_filter(self, field, terms):
        self.search = self._get_or_create_search().exclude(
            self._make_must_equal_terms_query(
                field=field,
                terms=terms
            )
        )

    def _add_must_not_equal_terms_post_filter(self, field, terms):
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_query(
                filter=[
                    ~self._make_must_equal_terms_query(
                        field=field,
                        terms=terms
                    )
                ]
            )
        )

    def _add_field_must_exist_filter(self, field):
        self.search = self._get_or_create_search().query(
            self._make_bool_query(
                filter=[
                    self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_exist_post_filter(self, field):
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_query(
                filter=[
                    self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_not_exist_filter(self, field):
        self.search = self._get_or_create_search().query(
            self._make_bool_query(
                filter=[
                    ~self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_not_exist_post_filter(self, field):
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_query(
                filter=[
                    ~self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_terms_aggregation(self, title, field, exclude=[], size=200):
        self._get_or_create_search().aggs.bucket(
            title,
            self._make_terms_aggregation(
                field=field,
                size=size,
                exclude=exclude
            )
        )

    def _add_exists_aggregation(self, title, field):
        self._get_or_create_search().aggs.bucket(
            title,
            self._make_exists_aggregation(
                field=field
            )
        )

    def add_query_string_query(self):
        query = self._get_query()
        if query:
            self.search = self._get_or_create_search().query(
                self._make_query_string_query(
                    query=query,
                    fields=self._get_search_fields(),
                    default_operator=AND
                )
            )

    def add_filters(self):
        '''
        These filters apply to the entire aggregation and result context.
        '''
        self.search = self._get_or_create_search().query(
            self._make_bool_query(
                must=self._make_default_filters()
            )
        )

    def add_aggregations_and_aggregation_filters(self):
        '''
        Each aggregation is computed in a filter context that filters
        everything but the params of the same type.
        '''
        params = self._get_post_filters()
        for facet_type, facet_options in self._get_facets():
            filtered_params = self.params_parser.get_not_keys_filters(
                not_keys=[facet_type],
                params=params
            )
            must, must_not, exists, not_exists = self._make_split_filter_queries(
                params=filtered_params
            )
            subaggregation = self._subaggregation_factory(
                facet_options.get(TYPE_KEY)
            )
            subaggregation = subaggregation(
                field=self._map_param_key_to_elasticsearch_field(facet_type),
                exclude=facet_options.get(EXCLUDE, []),
                #TODO: size should be defined in schema instead of long keyword.
                size=3000 if facet_options.get(LENGTH) == LONG else 200
            )
            agg = self._make_filter_and_subaggregation(
                title=facet_type.replace(PERIOD, DASH),
                filter_context=self._make_bool_query(
                    must=must + exists,
                    must_not= must_not + not_exists
                ),
                subaggregation=subaggregation
            )
            self._get_or_create_search().aggs.bucket(facet_options.get(TITLE), agg)
            

    def add_post_filters(self):
        '''
        These filters apply to the final results returned, after aggregation
        has been computed.
        '''
        must, must_not, exists, not_exists = self._make_split_filter_queries()
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_query(
                must=must + exists,
                must_not=must_not + not_exists
            )
        )

    def add_source(self):
        self.search = self._get_or_create_search().extra(
            **{
                _SOURCE: self._get_return_fields()
            }
        )

    def build_query(self):
        '''
        Public method to be implemented by children.
        '''
        raise NotImplementedError


class BasicSearchQueryFactory(AbstractQueryFactory):

    def __init__(self, params_parser, *args, **kwargs):
        super().__init__(params_parser, *args, **kwargs)

    def build_query(self):
        self.add_query_string_query()
        self.add_filters()
        self.add_post_filters()
        self.add_source()
        return self.search


class BasicSearchQueryFactoryWithFacets(BasicSearchQueryFactory):

    def __init__(self, params_parser, *args, **kwargs):
        super().__init__(params_parser, *args, **kwargs)

    def build_query(self):
        super().build_query()
        self.add_aggregations_and_aggregation_filters()
        return self.search
