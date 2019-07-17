from .interfaces import FACETS
from .interfaces import GRAPH
from .interfaces import TITLE
from .queries import BasicSearchQueryFactoryWithFacets
from .responses import BasicQueryResponseWithFacets


class ResponseField:
    '''
    Interface for defining a field in a FieldedResponse.
    '''

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.response = {}

    def render(self):
        '''
        Should implement field-specific logic and return dictionary
        with keys/values to update response.
        '''
        raise NotImplementedError


class BasicSearchWithFacetsResponseField(ResponseField):
    '''
    Returns formatted results from ES query.
    '''

    def __init__(self, *args, **kwargs):
        self.params_parser = kwargs.pop('params_parser', None)
        super().__init__(*args, **kwargs)
        self.query_builder = None
        self.query = None
        self.results = None

    def _build_query(self):
        self.query_builder = BasicSearchQueryFactoryWithFacets(
            params_parser=self.params_parser,
            **self.kwargs
        )
        self.query = self.query_builder.build_query()

    def _execute_query(self):
        self.results = BasicQueryResponseWithFacets(
            results=self.query.execute(),
            query_builder=self.query_builder
        )

    def _format_results(self):
        self.response.update(
            {
                GRAPH: self.results.to_graph(),
                FACETS: self.results.to_facets()
            }
        )

    def render(self):
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response


class RawSearchWithAggsResponseField(BasicSearchWithFacetsResponseField):
    '''
    Returns raw results from ES query.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _execute_query(self):
        self.results = self.query.execute()

    def _format_results(self):
        self.response.update(
            self.results.to_dict()
        )

    def render(self):
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response


class TitleResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        self.title = kwargs.pop('title', None)
        super().__init__(*args, **kwargs)

    def render(self):
        return {
            TITLE: self.title
        }
