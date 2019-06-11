

class ParamsParser():
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

    def get_filters_by_condition(self, key_condition=None, value_condition=None):
        return [
            (k, v) for k, v in self._request.params.items()
            if (key_condition is None or k == key_condition)
            and (value_condition is None or v == value_condition)
        ]
