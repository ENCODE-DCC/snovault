from .defaults import NOT_FILTERS
from elasticsearch_dsl import Search
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX


class AbstractQueryFactory():
    '''
    Interface for building specific queries.
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

    def _get_doc_types(self):
        return self.params_parser.get_type_filters()

    def _get_query(self):
        return self.params_parser.get_search_term_filters()

    def _get_filters(self):
        return self.params_parser.get_not_keys_filters(not_keys=NOT_FILTERS)

    def _get_post_filters(self):
        return self.kwargs.get('post_filters')

    def _get_sort(self):
        return self.params_parser.get_sort()

    def _get_limit(self):
        return self.params_parser.get_limit()

    def _get_search_fields(self):
        return self.kwargs.get('search_fields')

    def _get_return_fields(self):
        return self.params_parser.get_field_filters()

    def _get_facets(self):
        return self.kwargs.get('facets')

    def _get_facet_size(self):
        return self.kwargs.get('facet_size')

    def _add_query(self):
        pass

    def _add_filters(self):
        pass

    def _add_aggs(self):
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
        self._add_query()
        self._add_filters()
        self._add_aggs()
        return self.search
