from .interfaces import AUDIT
from .interfaces import EMBEDDED_FRAME
from .interfaces import OBJECT_FRAME


OPTIONAL_PARAMS = [
    'type',
    'limit',
    'mode',
    'annotation',
    'format',
    'frame',
    'datastore',
    'field',
    'region',
    'genome',
    'sort',
    'from',
    'referrer',
    'filterresponse',
    'remove',
    'cart',
    'debug',
]

FREE_TEXT_QUERIES = [
    'searchTerm',
    'advancedQuery',
]

NOT_FILTERS = OPTIONAL_PARAMS + FREE_TEXT_QUERIES

BASE_SEARCH_FIELDS = [
    '_all',
]

BASE_RETURN_FIELDS = [
    'embedded.@id',
    'embedded.@type',
]

BASE_COLUMNS = {
    '@id': {
        'title': 'ID'
    }
}

DEFAULT_COLUMNS = {
    'accession': {
        'title': 'Accession'
    },
    'aliases': {
        'title': 'Aliases'
    },
    'title': {
        'title': 'Title'
    },
    'description': {
        'title': 'Description'
    },
    'name': {
        'title': 'Name'
    }
}

BASE_FIELD_FACETS = [
    (
        'type',
        {
            'title': 'Data Type',
            'exclude': ['Item']
        }
    ),
]

BASE_AUDIT_FACETS = [
        ('audit.ERROR.category', {'title': 'Audit category: ERROR'}),
        ('audit.NOT_COMPLIANT.category', {'title': 'Audit category: NOT COMPLIANT'}),
        ('audit.WARNING.category', {'title': 'Audit category: WARNING'}),
]

INTERNAL_AUDIT_FACETS = [
    ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'}),
]

MAX_ES_RESULTS_WINDOW = 9999

DEFAULT_FRAMES = [
    EMBEDDED_FRAME,
    OBJECT_FRAME,
]

KEEP_LAYERED_FIELDS = [
    AUDIT,
]

DEFAULT_SORT_OPTIONS = {'order': 'desc', 'unmapped_type': 'keyword'}

DEFAULT_SORT = [
    {'embedded.date_created': {'order': 'desc', 'unmapped_type': 'keyword'}},
    {'embedded.label': {'order': 'desc', 'unmapped_type': 'keyword'}},
    {'embedded.uuid': {'order': 'desc', 'unmapped_type': 'keyword'}},
]

AUDIT_FIELDS = [
    'audit.ERROR.category',
    'audit.NOT_COMPLIANT.category',
    'audit.WARNING.category',
    'audit.INTERNAL_ACTION.category',
]

DEFAULT_TERMS_AGGREGATION_KWARGS = [
    'size',
    'exclude',
    'missing',
]

DEFAULT_EXISTS_AGGREGATION_KWARGS = []
