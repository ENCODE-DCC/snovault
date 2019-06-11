

class ParamsParser():
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

    def get_filters_by_condition(self, key_condition=None, value_condition=None):
        '''
        Condition must be function that accepts key or value and returns bool.
        '''
        return [
            (k, v) for k, v in self._request.params.items()
            if (key_condition is None or key_condition(k))
            and (value_condition is None or value_condition(v))
        ]
