

class ParamsParser():
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

