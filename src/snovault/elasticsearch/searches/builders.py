from .defaults import NOT_FILTERS
from elasticsearch_dsl import Search
from snovault.elasticsearch import ELASTIC_SEARCH
from snovault.elasticsearch.interfaces import RESOURCES_INDEX


class ResponseBuilder():

    def __init__(self, response_fields=[]):
        self.response = {}
        self.response_fields = response_fields
        self.validate_response_fields()

    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render())
        return self.response


class QueryBuilder():
    '''
    Interface for building specific queries.
    '''
    search = None

    def __init__(self, params_parser, *args, **kwargs):
        self.params_parser = params_parser
        self.args = args
        self.kwargs = kwargs

    def _get_search(self):
        if self.search is None:
            self.search = Search(
                using=self._get_client,
                index=self._get_index,
            )
        return self.search

    def _get_client(self):
        return self.params_parser._request.registry[ELASTIC_SEARCH]

    def _get_index(self):
        return RESOURCES_INDEX

    def _get_doc_types(self):
        return self.params_parser.get_type_filters()

    def _get_query(self):
        return self.params_parser.get_search_term_filters()

    def _get_filters(self):
        return self.params_parser.get_not_keys_filters(not_keys=NOT_FILTERS)

    def _get_post_filters(self):
        return None

    def _get_sort(self):
        return self.params_parser.get_sort()

    def _get_size(self):
        return self.params_parser.get_limit()

    def _get_search_fields(self):
        return None

    def _get_return_fields(self):
        return self.params_parser.get_field_filters()

    def _get_facets(self):
        return None

    def _get_facet_size(self):
        return None

    def _new_search(self):
        self.search = self._get_search()

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


class BasicSearchQuery(QueryBuilder):

    def __init__(self, *args, **kwargs):
        super.__init__(args, kwargs)
    
    def build_query(self):
        self._new_search()
        self._add_query()
        self._add_filters()
        self._add_aggs()
        return self.search
