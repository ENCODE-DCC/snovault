from .interfaces import NOT_FLAG
from .interfaces import TYPE_KEY


class ParamsParser():
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

    def is_param(self, key, value):
        '''
        Returns True if key and value pair exist in params, otherwise False.
        '''
        return (key, value) in self._request.params.items()

    def get_filters_by_condition(self, key_condition=None, value_condition=None, params=None):
        '''
        Condition must be function that accepts key or value and returns bool. Optional params
        kwarg allows chaining of filters.
        '''
        if params is None:
            params = self._request.params.items()
        return [
            (k, v) for k, v in params
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

    def get_type_filters(self, params=None):
        return self.get_key_filters(
            key=TYPE_KEY,
            params=params
        )

    def get_must_match_filters(self, params=None):
        return self.get_filters_by_condition(
            key_condition=lambda k: NOT_FLAG not in k,
            params=params
        )

    def get_must_not_match_filters(self, params=None):
        return self.get_filters_by_condition(
            key_condition=lambda k: NOT_FLAG in k,
            params=params
        )
