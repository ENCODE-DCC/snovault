class AbstractResponseField:
    '''
    Interface for defining a field in a response.
    '''

    def __init__(self, field_name=None):
        self.field_name = field_name

    def render(self):
        '''
        Should implement field-specific logic and return dictionary
        with keys/values to update response.
        '''
        raise NotImplementedError
