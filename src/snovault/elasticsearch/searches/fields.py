from snosearch.fields import ResponseField
from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from snovault.elasticsearch.searches.interfaces import NON_SORTABLE


class NonSortableResponseField(ResponseField):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        self.parent = kwargs.get('parent')
        return {
            NON_SORTABLE: TEXT_FIELDS
        }


class PassThroughResponseField(ResponseField):
    '''
    Passes input values (dictionary) to output.
    '''
    def __init__(self, *args, **kwargs):
        self.values_to_pass_through = kwargs.pop('values_to_pass_through', {})
        super().__init__(*args, **kwargs)

    def render(self, *args, **kwargs):
        return self.values_to_pass_through
