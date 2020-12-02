from collections import OrderedDict
from snovault.elasticsearch.create_mapping import TEXT_FIELDS

from .interfaces import ALL
from .interfaces import AT_ID
from .interfaces import AT_CONTEXT
from .interfaces import AT_TYPE
from .interfaces import CLEAR_FILTERS
from .interfaces import COLUMNS
from .interfaces import DEBUG_KEY
from .interfaces import EMBEDDED
from .interfaces import FACETS
from .interfaces import FIELD_KEY
from .interfaces import FILTERS
from .interfaces import GRAPH
from .interfaces import HITS
from .interfaces import JSONLD_CONTEXT
from .interfaces import LIMIT_KEY
from .interfaces import MATRIX
from .interfaces import NON_SORTABLE
from .interfaces import NO_RESULTS_FOUND
from .interfaces import NOTIFICATION
from .interfaces import RAW_QUERY
from .interfaces import REMOVE
from .interfaces import SEARCH_BASE
from .interfaces import SEARCH_PATH
from .interfaces import SORT_KEY
from .interfaces import SUCCESS
from .interfaces import TERM
from .interfaces import TITLE
from .interfaces import TOTAL
from .queries import AuditMatrixQueryFactoryWithFacets
from .queries import BasicMatrixQueryFactoryWithFacets
from .queries import BasicSearchQueryFactory
from .queries import BasicSearchQueryFactoryWithFacets
from .queries import BasicReportQueryFactoryWithFacets
from .queries import CollectionSearchQueryFactoryWithFacets
from .queries import MissingMatrixQueryFactoryWithFacets
from .responses import AuditMatrixResponseWithFacets
from .responses import BasicMatrixResponseWithFacets
from .responses import BasicQueryResponseWithFacets
from .responses import RawQueryResponseWithAggs


class ResponseField:
    '''
    Interface for defining a field in a FieldedResponse.
    '''

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.response = {}
        self.parent = None

    def _get_meta_field(self, meta_field):
        if self.parent:
            return self.parent._meta.get(meta_field)

    def get_params_parser(self):
        return self._get_meta_field('params_parser')

    def get_request(self):
        params_parser = self.get_params_parser()
        if params_parser:
            return params_parser._request

    def get_query_builder(self):
        return self._get_meta_field('query_builder')

    def render(self, *args, **kwargs):
        '''
        Should implement field-specific logic and return dictionary
        with keys/values to update response.
        '''
        raise NotImplementedError


class BasicSearchResponseField(ResponseField):
    '''
    Builds faster query (no aggregations) and returns formatted hits.
    For use when aggregations are not needed.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query_builder = None
        self.query = None
        self.results = None

    def _build_query(self):
        self.query_builder = BasicSearchQueryFactory(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _register_query(self):
        '''
        Adds to parent _meta in case other fields need to use.
        '''
        self.parent._meta['query_builder'] = self.query_builder

    def _execute_query(self):
        self.results = BasicQueryResponseWithFacets(
            results=self.query.execute(),
            query_builder=self.query_builder
        )

    def _format_results(self):
        self.response.update(
            {
                GRAPH: self.results.to_graph()
            }
        )

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._build_query()
        self._register_query()
        self._execute_query()
        self._format_results()
        return self.response


class BasicSearchWithFacetsResponseField(BasicSearchResponseField):
    '''
    Like BasicSearchResponseField but builds query with aggregations and renders
    facets and other frontend fields. This is the standard search result response.
    '''

    def _build_query(self):
        self.query_builder = BasicSearchQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _format_results(self):
        self.response.update(
            {
                GRAPH: self.results.to_graph(),
                FACETS: self.results.to_facets(),
                TOTAL: self.results.results.hits.total
            }
        )


class CollectionSearchWithFacetsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Like BasicSearchWithFacetsResponseField but uses CollectionSearchQueryFactoryWithFacets
    as query builder.
    '''

    def _build_query(self):
        self.query_builder = CollectionSearchQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()


class RawSearchWithAggsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Like BasicSearchWithFacetsResponseField but returns raw results from ES query.
    Useful for debugging/building frontend views that are less reliant on backend logic.
    '''

    def _execute_query(self):
        self.results = RawQueryResponseWithAggs(
            results=self.query.execute(),
            query_builder=self.query_builder
        )

    def _format_results(self):
        self.response.update(
            self.results.results.to_dict()
        )

    def _maybe_scan_over_results(self):
        # Required if ES didn't return all the hits in one response.
        if self.query_builder._should_scan_over_results():
            self.response[HITS][HITS] = self.results.to_graph()

    def render(self, *args, **kwargs):
        super().render(*args, **kwargs)
        self._maybe_scan_over_results()
        return self.response


class BasicReportWithFacetsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Like BasicSearchWithFacetsResponseField but uses BasicReportQueryFactoryWithFacet
    query builder.
    '''

    def _build_query(self):
        self.query_builder = BasicReportQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()


class RawMatrixWithAggsResponseField(BasicSearchResponseField):
    '''
    Matrix has no hits so just returns raw aggregations.
    '''

    def _build_query(self):
        self.query_builder = BasicMatrixQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _execute_query(self):
        self.results = self.query.execute()

    def _format_results(self):
        self.response.update(
            self.results.to_dict()
        )


class BasicMatrixWithFacetsResponseField(RawMatrixWithAggsResponseField):
    '''
    Like RawMatrixWithAggsResponseField but formats facets and matrix.
    '''

    def _execute_query(self):
        self.results = BasicMatrixResponseWithFacets(
            results=self.query.execute(),
            query_builder=self.query_builder
        )

    def _format_results(self):
        self.response.update(
            {
                FACETS: self.results.to_facets(),
                MATRIX: self.results.to_matrix(),
                TOTAL: self.results.results.hits.total
            }
        )


class MissingMatrixWithFacetsResponseField(BasicMatrixWithFacetsResponseField):
    '''
    Like BasicMatrixWithAggsResponseField but uses MissingMatrixQueryFactoryWithFacets
    query.
    '''

    def _build_query(self):
        self.query_builder = MissingMatrixQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()


class AuditMatrixWithFacetsResponseField(BasicMatrixWithFacetsResponseField):
    '''
    Like BasicMatrixWithFacetsResponseField but uses AuditMatrixQueryFactoryWithFacet
    and AuditMatrixResponseWithFacets.
    '''

    def _build_query(self):
        self.query_builder = AuditMatrixQueryFactoryWithFacets(
            params_parser=self.get_params_parser(),
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _execute_query(self):
        self.results = AuditMatrixResponseWithFacets(
            results=self.query.execute(),
            query_builder=self.query_builder
        )


class TitleResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        self.title = kwargs.pop('title', None)
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        return {
            TITLE: self.title
        }


class TypeResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        self.at_type = kwargs.pop('at_type', None)
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        return {
            AT_TYPE: self.at_type
        }


class ContextResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            AT_CONTEXT: self.get_request().route_path(JSONLD_CONTEXT)
        }


class IDResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            AT_ID: self.get_request().path_qs
        }


class AllResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_limit(self):
        return self.parent._meta['query_builder']._get_limit()

    def _get_qs_with_limit_all(self):
        return self.get_params_parser().get_query_string(
            params=self.get_params_parser().get_not_keys_filters(
                not_keys=[LIMIT_KEY]
            ) + [(LIMIT_KEY, ALL)]
        )

    def _maybe_add_all(self):
        conditions = [
            not self.get_query_builder()._limit_is_all(),
            self.get_query_builder()._get_limit_value_as_int() < self.parent.response.get(TOTAL, 0)
        ]
        if all(conditions):
            self.response.update(
                {
                    ALL: self.get_request().path + '?' + self._get_qs_with_limit_all()
                }
            )

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._maybe_add_all()
        return self.response


class NotificationResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _results_found(self):
        if self.parent.response.get('total'):
            return True
        return False

    def _set_notification(self, message):
        self.response[NOTIFICATION] = message

    def _set_status_code(self, status_code):
        self.get_request().response.status_code = status_code

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        if self._results_found():
            self._set_notification(SUCCESS)
        else:
            self._set_notification(NO_RESULTS_FOUND)
            self._set_status_code(404)
        return self.response


class FiltersResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filters = []

    def _get_filters_and_search_terms_from_query_string(self):
        return (
            self.get_query_builder()._get_post_filters()
            + self.get_params_parser().get_search_term_filters()
            + self.get_params_parser().get_advanced_query_filters()
        )

    def _get_path_qs_without_filter(self, key, value):
        path = self.get_request().path
        remove_qs = self.get_params_parser().get_query_string(
            params=self.get_params_parser().remove_key_and_value_pair_from_filters(
                key=key,
                value=value
            )
        )
        if remove_qs:
            path += '?' + remove_qs
        return path

    def _make_filter(self, key, value):
        filter_entry = OrderedDict(
            {
                FIELD_KEY: key,
                TERM: value,
                REMOVE: self._get_path_qs_without_filter(key, value)
            }
        )
        self.filters.append(filter_entry)

    def _make_filters(self):
        for key, value in self._get_filters_and_search_terms_from_query_string():
            self._make_filter(key, value)
        self.response[FILTERS] = self.filters

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._make_filters()
        return self.response


class ClearFiltersResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_search_term_or_types_from_query_string(self):
        params_parser = self.get_params_parser()
        return params_parser.get_search_term_filters() or params_parser.get_type_filters()

    def _get_path_qs_with_no_filters(self):
        path = self.get_request().path
        no_filters_qs = self.get_params_parser().get_query_string(
            params=self._get_search_term_or_types_from_query_string()
        )
        if no_filters_qs:
            path += '?' + no_filters_qs
        return path

    def _add_clear_filters(self):
        self.response = {
            CLEAR_FILTERS: self._get_path_qs_with_no_filters()
        }

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._add_clear_filters()
        return self.response


class TypeOnlyClearFiltersResponseField(ClearFiltersResponseField):
    '''
    Like ClearFiltersResponseField but always returns types even if
    searchTerm is in query string.
    '''

    def _get_search_term_or_types_from_query_string(self):
        return self.get_params_parser().get_type_filters()


class CollectionClearFiltersResponseField(ClearFiltersResponseField):
    '''
    Like ClearFiltersResponseField but redirects to search page and
    gets item_type from collection context.
    '''

    def _get_search_term_or_types_from_query_string(self):
        return self.get_query_builder()._get_item_types()

    def _get_path_qs_with_no_filters(self):
        path = SEARCH_PATH
        no_filters_qs = self.get_params_parser().get_query_string(
            params=self._get_search_term_or_types_from_query_string()
        )
        if no_filters_qs:
            path += '?' + no_filters_qs
        return path


class DebugQueryResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        '''
        Returns constructed query in debug field if debug param specified.
        '''
        self.parent = kwargs.get('parent')
        if self.get_params_parser().get_debug():
            self.response.update(
                {
                    DEBUG_KEY: {
                        RAW_QUERY: self.get_query_builder().search.to_dict()
                    }
                }
            )
        return self.response


class ColumnsResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            COLUMNS: self.get_query_builder()._get_columns_for_item_types()
        }


class NonSortableResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            NON_SORTABLE: TEXT_FIELDS
        }


class SortResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _get_sort_from_query(self):
        return self.get_query_builder().search._sort

    def _remove_prefix(self, sort_by, prefix=EMBEDDED):
        return OrderedDict(
            [
                (k.replace(prefix, ''), v)
                for sort in sort_by
                for k, v in sort.items()
            ]
        )

    def _maybe_add_sort(self):
        sort_by = self._get_sort_from_query()
        if sort_by:
            self.response.update(
                {
                    SORT_KEY: self._remove_prefix(sort_by)
                }
            )

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._maybe_add_sort()
        return self.response


class SearchBaseResponseField(ResponseField):
    '''
    Used for redirecting from matrix page to search page.
    (Same query_string, different path.)
    '''

    def __init__(self, search_base=SEARCH_PATH, *args, **kwargs):
        self.search_base = search_base
        super().__init__(*args, **kwargs)

    def _get_search_base(self):
        search_base = self.search_base
        qs = self.get_request().query_string
        if qs:
            search_base += '?' + qs
        return search_base

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            SEARCH_BASE: self._get_search_base()
        }
