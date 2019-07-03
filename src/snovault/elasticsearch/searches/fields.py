from .interfaces import GRAPH
from .queries import BasicSearchQueryFactoryWithFacets


class ResponseField:
    '''
    Interface for defining a field in a response.
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

    def __init__(self, *args, **kwargs):
        self.params_parser = kwargs.pop('params_parser', None)
        super().__init__(*args, **kwargs)
        self.query = None
        self.results = None

    def _build_query(self):
        bsq = BasicSearchQueryFactoryWithFacets(
            self.params_parser,
            **self.kwargs
        )
        self.query = bsq.build_query()

    def _execute_query(self, query):
        self.results = self.query.execute()

    def _format_results(self):
        self.response.update(
            {
                GRAPH: list(self.results),
                'aggs': self.results.aggs
            }
        )

    def render(self):
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response
