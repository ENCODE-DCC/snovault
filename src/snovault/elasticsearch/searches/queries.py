from itertools import chain

from .defaults import NOT_FILTERS
from .defaults import BASE_SEARCH_FIELDS
from .interfaces import AND
from .interfaces import AND_JOIN
from .interfaces import AND_NOT_JOIN
from .interfaces import BOOL
from .interfaces import BOOST_VALUES
from .interfaces import EMBEDDED
from .interfaces import EXISTS
from .interfaces import FILTERS
from .interfaces import NOT_JOIN
from .interfaces import NO
from .interfaces import QUERY_STRING
from .interfaces import TERMS
from .interfaces import YES
from elasticsearch_dsl import A
from elasticsearch_dsl import Q
from elasticsearch_dsl import Search
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX
from snovault.interfaces import TYPES


class AbstractQueryFactory():
    '''
    Interface for building specific queries. Don't change functionality here, instead
    inherit and extend/override functions as needed.
    '''
    search = None

    def __init__(self, params_parser, *args, **kwargs):
        self.params_parser = params_parser
        self.args = args
        self.kwargs = kwargs

    def _get_or_create_search(self):
        if self.search is None:
            self.search = Search(
                using=self._get_client,
                index=self._get_index,
            )
        return self.search

    def _get_client(self):
        return self.kwargs.get('client') or self.params_parser._request.registry[ELASTIC_SEARCH]

    def _get_index(self):
        return RESOURCES_INDEX

    def _get_item_types(self):
        return self.params_parser.get_type_filters()

    def _get_default_item_types(self):
        return self.kwargs.get('default_item_types', [])

    def _get_query(self):
        return self._combine_search_term_queries(
            must_match_filters=self.params_parser.get_must_match_search_term_filters(),
            must_not_match_filters=self.params_parser.get_must_not_match_search_term_filters()
        )

    def _get_filters(self):
        return self.params_parser.get_not_keys_filters(not_keys=NOT_FILTERS)

    def _get_post_filters(self):
        return self.kwargs.get('post_filters', [])

    def _get_sort(self):
        return self.params_parser.get_sort()

    def _get_limit(self):
        return self.params_parser.get_limit()

    def _get_search_fields(self):
        search_fields = set()
        search_fields.update(BASE_SEARCH_FIELDS)
        for item_type in chain(
                self.params_parser.param_values_to_list(self._get_item_types()),
                self._get_default_item_types()
        ):
            search_fields.update(
                self._prefix_values(
                    EMBEDDED,
                    self._get_boost_values_from_item_type(item_type).keys()
                )
            )
        return list(search_fields)

    def _get_return_fields(self):
        return self.params_parser.get_field_filters()

    def _get_facets(self):
        return self.kwargs.get('facets', [])

    def _get_facet_size(self):
        return self.kwargs.get('facet_size')

    def _get_boost_values_from_item_type(self, item_type):
        return self.params_parser._request.registry[TYPES][item_type].schema.get(
            BOOST_VALUES,
            {}
        )

    def _prefix_values(self, prefix, values):
        return [prefix + v for v in values]

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

    def _make_bool_filter_query(self, filter=None):
        return Q(
            BOOL,
            filter=filter
        )

    def _make_must_equal_terms_query(self, field, terms):
        return Q(
            TERMS,
            **{field: terms}
        )

    def _make_field_must_exist_query(self, field):
        return Q(
            EXISTS,
            field=field
        )

    def _make_terms_aggregation(self, field, exclude=[], size=200):
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

    def _make_filter_aggregation(self, filter_context):
        return A(
            'filter',
            filter_context
        )

    def _make_filter_and_sub_aggregation(self, title, filter_context, sub_aggregation):
        a = self._make_filter_aggregation(filter_context)
        a.bucket(title, sub_aggregation)
        return a

    def _add_query_string_query(self):
        query = self._get_query()
        if query:
            self.search = self._get_or_create_search().query(
                self._make_query_string_query(
                    query=query,
                    fields=self._get_search_fields(),
                    default_operator=AND
                )
            )

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
            self._make_bool_filter_query(
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
            self._make_bool_filter_query(
                filter=[
                    self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_exist_post_filter(self, field):
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_filter_query(
                filter=[
                    self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_not_exist_filter(self, field):
        self.search = self._get_or_create_search().query(
            self._make_bool_filter_query(
                filter=[
                    ~self._make_field_must_exist_query(field=field),
                ]
            )
        )

    def _add_field_must_not_exist_post_filter(self, field):
        self.search = self._get_or_create_search().post_filter(
            self._make_bool_filter_query(
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

    def _add_filters_and_sub_aggregations(self, title, field):
        pass

    def _add_filters(self):
        pass

    def _add_aggs(self):
        pass

    def _add_source(self):
        pass

    def build_query(self):
        '''
        Public method to be implemented by children.
        '''
        raise NotImplementedError


class BasicSearchQueryFactory(AbstractQueryFactory):

    def __init__(self, params_parser, *args, **kwargs):
        super().__init__(params_parser, *args, **kwargs)

    def build_query(self):
        self._get_or_create_search()
        self._add_query_string_query()
        self._add_filters()
        self._add_aggs()
        self._add_source()
        return self.search
