import json

from collections import OrderedDict
from pyramid.response import Response
from types import GeneratorType

from .decorators import remove_from_return
from .interfaces import APPLICATION_JSON
from .mixins import AggsToFacetsMixin
from .mixins import AggsToMatrixMixin
from .mixins import AuditAggsToMatrixMixin
from .mixins import HitsToGraphMixin
from .mixins import RawHitsToGraphMixin


class FieldedResponse:
    '''
    Returns rendered ResponseFields.
    '''

    def __init__(self, response_fields=[], _meta={}):
        self._meta = _meta
        self.response = {}
        self.response_fields = response_fields
        self.validate_response_fields()

    @property
    def ordered_response(self):
        return OrderedDict(sorted(self.response.items()))

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

    def get_request(self):
        params_parser = self._meta.get('params_parser')
        if params_parser:
            return params_parser._request

    def get_or_create_response(self):
        request = self.get_request()
        if request:
            return request.response
        return Response()

    def _is_request_from_embed(self):
        request = self.get_request()
        if request:
            return request.__parent__ is not None

    def _is_response_with_generator(self):
        return any(
            isinstance(v, GeneratorType)
            for v in self.response.values()
        )
    
    def _should_stream_response(self):
        return all(
            [
                not self._is_request_from_embed(),
                self._is_response_with_generator()
            ]
        )

    def _response_factory(self):
        if self._should_stream_response():
            response = StreamedResponse(self)
        else:
            response = InMemoryResponse(self)
        return response

    @remove_from_return(values=[None])
    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render(parent=self))
        return self._response_factory().render()


class FieldedGeneratorResponse(FieldedResponse):
    '''
    Like FieldedResponse but always returns a GeneratorResponse.
    '''

    def __init__(self, response_fields=[], _meta={}):
        super().__init__(response_fields=response_fields, _meta=_meta)

    def _response_factory(self):
        response = GeneratorResponse(self)
        return response


class StreamedResponse:
    '''
    Streams FieldedResponse generators that would otherwise run the machine
    out of memory.
    '''

    def __init__(self, fielded_response):
        self.fielded_response = fielded_response

    def _start_dict(self):
        return '{'

    def _end_dict(self):
        return '}'

    def _start_list(self):
        return '['

    def _end_list(self):
        return ']'

    def _comma(self):
        return ','

    def _colon(self):
        return ':'

    def _to_json(self, value):
        return json.dumps(value)

    def _to_json_from_generator(self, generator):
        yield self._start_list()
        for i, v in enumerate(generator):
            if i > 0:
                yield self._comma()
            yield self._to_json(v)
        yield self._end_list()

    def _iter(self):
        yield self._start_dict()
        for i, (k, v) in enumerate(self.fielded_response.ordered_response.items()):
            if i > 0:
                yield self._comma()
            yield self._to_json(k)
            yield self._colon()
            if isinstance(v, GeneratorType):
                yield from self._to_json_from_generator(v)
            else:
                yield self._to_json(v)
        yield self._end_dict()

    def __iter__(self):
        yield from (
            s.encode('utf-8')
            for s in self._iter()
        )

    def _make_streamed_response(self):
        response = self.fielded_response.get_or_create_response()
        response.content_type=APPLICATION_JSON
        response.app_iter=self
        return response

    def render(self):
        return self._make_streamed_response()


class InMemoryResponse:
    '''
    Renders FieldedResponse generators in memory.
    '''

    def __init__(self, fielded_response):
        self.fielded_response = fielded_response

    def render(self):
        return {
            k: list(v) if isinstance(v, GeneratorType) else v
            for k, v in self.fielded_response.ordered_response.items()
        }


class GeneratorResponse:
    '''
    Returns only raw FieldedResponse generators.
    '''

    def __init__(self, fielded_response):
        self.fielded_response = fielded_response

    def render(self):
        return {
            k: v
            for k, v in self.fielded_response.ordered_response.items()
            if isinstance(v, GeneratorType)
        }


class QueryResponse:
    '''
    Holds results and allows mixin of aggregation and hits formatters.
    '''

    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.results = results
        self.query_builder = query_builder


class BasicQueryResponseWithFacets(QueryResponse, HitsToGraphMixin, AggsToFacetsMixin):
    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(results, query_builder, *args, **kwargs)


class RawQueryResponseWithAggs(QueryResponse, RawHitsToGraphMixin):
    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(results, query_builder, *args, **kwargs)


class BasicMatrixResponseWithFacets(QueryResponse, AggsToFacetsMixin, AggsToMatrixMixin):
    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(results, query_builder, *args, **kwargs)


class AuditMatrixResponseWithFacets(QueryResponse, AggsToFacetsMixin, AuditAggsToMatrixMixin):
    def __init__(self, results, query_builder, *args, **kwargs):
        super().__init__(results, query_builder, *args, **kwargs)
