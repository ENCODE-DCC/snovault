from .fields import AbstractResponseField


class ResponseBuilder():

    def __init__(self, response_fields=[]):
        self.response = {}
        self.response_fields = response_fields
        self.validate_response_fields()

    def validate_response_fields(self):
        for f in self.response_fields:
            if not isinstance(f, AbstractResponseField):
                raise ValueError(
                    '{} must be of type AbstractResponseField'.format(
                        f.__class__.__name__
                    )
                )

    def render(self):
        '''
        Expects response_fields will return dictionaries with unique keys.
        '''
        for f in self.response_fields:
            self.response.update(f.render())
        return self.response
