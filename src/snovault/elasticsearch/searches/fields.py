from .builders import BasicSearchQuery


class AbstractResponseField:
    '''
    Interface for defining a field in a response.
    '''
    response = {}
    
    def __init__(self, field_name=None):
        self.field_name = field_name

    def render(self):
        '''
        Should implement field-specific logic and return dictionary
        with keys/values to update response.
        '''
        raise NotImplementedError


class BasicSearchResponseField(AbstractResponseField):

    def __init__(self, field_name=None, params_parser=None):
        self.params_parser = params_parser
        super().__init__(field_name)

    def _build_query(self):
        bsq = BasicSearchQuery(self.params_parser)

    def _execute_query(self):
        pass

    def _format_results(self):
        pass

    def render(self):
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response
