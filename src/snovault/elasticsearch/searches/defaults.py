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
]

FREE_TEXT_QUERIES = [
    'searchTerm',
    'advancedQuery',
]

NOT_FILTERS = OPTIONAL_PARAMS + FREE_TEXT_QUERIES

BASE_SEARCH_FIELDS = [
    '_all'
]

BASE_RETURN_FIELDS = [
    'embedded.@id',
    'embedded.@type',
]

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
    ('audit.INTERNAL_ACTION.category', {'title': 'Audit category: DCC ACTION'})
]

MAX_ES_RESULTS_WINDOW = 9999
