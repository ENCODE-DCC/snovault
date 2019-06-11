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

    def get_filters_by_condition(self, key_condition=None, value_condition=None):
        '''
        Condition must be function that accepts key or value and returns bool.
        '''
        return [
            (k, v) for k, v in self._request.params.items()
            if (key_condition is None or key_condition(k))
            and (value_condition is None or value_condition(v))
        ]

    def get_type_filters(self):
        return self.get_filters_by_condition(key_condition=lambda k: k == TYPE_KEY)
