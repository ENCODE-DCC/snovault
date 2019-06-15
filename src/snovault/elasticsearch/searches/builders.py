class ResponseBuilder():

    def __init__(self, response_fields=[]):
        self.response = {}
        self.response_fields = response_fields

    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render())
        return self.response
