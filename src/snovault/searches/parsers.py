from .interfaces import FIELD_KEY
from .interfaces import LIMIT_KEY
from .interfaces import NOT_FLAG
from .interfaces import SEARCH_TERM_KEY
from .interfaces import SIZE_KEY
from .interfaces import TYPE_KEY
from urllib.parse import urlencode


class ParamsParser():
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

    def _params(self, params=None):
        if params is None:
            params = list(self._request.params.items())
        return params

    def is_param(self, key, value, params=None):
        '''
        Returns True if key and value pair exist in params, otherwise False.
        '''
        return (key, value) in self._params(params)

    def get_filters_by_condition(self, key_condition=None, value_condition=None, params=None):
        '''
        Condition must be function that accepts key or value and returns bool. Optional params
        kwarg allows chaining of filters.
        '''
        return [
            (k, v) for k, v in self._params(params)
            if (key_condition is None or key_condition(k))
            and (value_condition is None or value_condition(v))
        ]

    def get_key_filters(self, key=None, params=None):
        '''
        Returns params with specified key (= and !=).
        '''
        return self.get_filters_by_condition(
            key_condition=lambda k: k is None or k == key or k == key + NOT_FLAG,
            params=params
        )

    def get_keys_filters(self, keys=[], params=None):
        '''
        Returns keys contained in keys list (= and !=).
        '''
        return self.get_filters_by_condition(
            key_condition=lambda k: k in keys or k.replace(NOT_FLAG, '') in keys,
            params=params
        )

    def get_not_keys_filters(self, not_keys=[], params=None):
        '''
        Returns keys not contained in not_keys list (= and !=).
        '''
        return self.get_filters_by_condition(
            key_condition=lambda k: k not in not_keys and k.replace(NOT_FLAG, '') not in not_keys,
            params=params
        )

    def get_query_string(self, params=None):
        '''
        Can be called at end of filter chain to return urlencoded string.
        '''
        return urlencode(self._params(params), doseq=True)

    def get_must_match_filters(self, params=None):
        return self.get_filters_by_condition(
            key_condition=lambda k: not k.endswith(NOT_FLAG),
            params=params
        )

    def get_must_not_match_filters(self, params=None):
        return self.get_filters_by_condition(
            key_condition=lambda k: k.endswith(NOT_FLAG),
            params=params
        )

    def get_type_filters(self, params=None):
        return self.get_key_filters(
            key=TYPE_KEY,
            params=params
        )

    def get_search_term_filters(self, params=None):
        return self.get_key_filters(
            key=SEARCH_TERM_KEY,
            params=params
        )

    def get_field_filters(self, params=None):
        return self.get_key_filters(
            key=FIELD_KEY,
            params=params
        )

    def get_size(self, params=None):
        return self.get_key_filters(
            key=SIZE_KEY,
            params=params
        )

    def get_limit(self, params=None):
        return self.get_key_filters(
            key=LIMIT_KEY,
            params=params
        )
