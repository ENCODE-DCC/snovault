from .decorators import remove_from_return
from .mixins import AggsToFacetsMixin
from .mixins import HitsToGraphMixin


class FieldedResponse:
    '''
    Returns rendered ResponseFields.
    '''
    def __init__(self, response_fields=[]):
        self.response = {}
        self.response_fields = response_fields
        self.validate_response_fields()

    def validate_response_fields(self):
        from .fields import ResponseField
        for f in self.response_fields:
            if not isinstance(f, ResponseField):
                raise ValueError(
                    '{} must be of type {}'.format(
                        f.__class__.__name__,
                        ResponseField.__name__
                    )
                )

    @remove_from_return(values=[None])
    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render())
        return self.response


class QueryResponse:
    '''
    Holds results and allows mixin of aggregation and hits formatters.
    '''

    def __init__(self, results, params_parser):
        self.results = results
        self.params_parser = params_parser


class BasicQueryResponseWithFacets(QueryResponse, HitsToGraphMixin, AggsToFacetsMixin):

    def __init__(self, results, params_parser, *args, **kwargs):
        super().__init__(results, params_parser)
