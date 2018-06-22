from snovault import (
    Item,
    collection,
)

# Test class for keys


def includeme(config):
    config.scan(__name__)


@collection(
    'testing-keys',
    properties={
        'title': 'Test keys',
        'description': 'Testing. Testing. 1, 2, 3.',
    },
    unique_key='testing_alias',
)
class TestingKey(Item):
    item_type = 'testing_key'
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
                'uniqueKey': True,
            },
            'alias': {
                'type': 'string',
                'uniqueKey': 'testing_alias',
            },
        }
    }
