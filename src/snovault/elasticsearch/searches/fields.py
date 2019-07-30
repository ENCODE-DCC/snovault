from .interfaces import ALL
from .interfaces import AT_ID
from .interfaces import AT_CONTEXT
from .interfaces import AT_TYPE
from .interfaces import FACETS
from .interfaces import GRAPH
from .interfaces import LIMIT_KEY
from .interfaces import JSONLD_CONTEXT
from .interfaces import TITLE
from .interfaces import TOTAL
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
        self.parent = None

    def get_params_parser(self):
        if self.parent:
            return self.parent._meta.get('params_parser')

    def get_request(self):
        params_parser = self.get_params_parser()
        if params_parser:
            return params_parser._request

    def render(self, *args, **kwargs):
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
        super().__init__(*args, **kwargs)
        self.query_builder = None
        self.query = None
        self.results = None

    def _build_query(self):
        self.query_builder = BasicSearchQueryFactoryWithFacets(
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
                GRAPH: self.results.to_graph(),
                FACETS: self.results.to_facets(),
                TOTAL: self.results.results.hits.total
            }
        )

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._build_query()
        self._register_query()
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

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._build_query()
        self._execute_query()
        self._format_results()
        return self.response


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
        params_parser = self.get_params_parser()
        return params_parser.get_query_string(
            params=params_parser.get_not_keys_filters(
                not_keys=[LIMIT_KEY]
            ) + [(LIMIT_KEY, ALL)]
        )

    def _maybe_add_all(self):
        limit = self.get_params_parser().param_values_to_list(
            params=self._get_limit()
        )
        if limit and limit[0] < self.parent.response.get(TOTAL, 0):
            self.response.update(
                {
                    ALL: self.get_request().path + self._get_qs_with_limit_all()
                }
            )

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        self._maybe_add_all()
        return self.response
