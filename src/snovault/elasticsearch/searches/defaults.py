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
    '_all',
    '*.uuid',
    '*.md5sum',
    '*.submitted_file_name',
    'unique_keys.*',
]
BASE_FACETS = [
    ('type', {'title': 'Data Type'}),
]
