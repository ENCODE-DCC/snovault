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
