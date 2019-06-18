from .queries import BasicSearchQueryFactory


class ResponseField:
    '''
    Interface for defining a field in a response.
    '''
    response = {}

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def render(self):
        '''
        Should implement field-specific logic and return dictionary
        with keys/values to update response.
        '''
        raise NotImplementedError


class BasicSearchResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        self.params_parser = kwargs.pop('params_parser', None)
        super().__init__(*args, **kwargs)

    def _build_query(self):
        bsq = BasicSearchQueryFactory(
            self.params_parser,
            **self.kwargs
        )

    def _execute_query(self):
        pass

    def _format_results(self):
        pass

    def render(self):
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response
