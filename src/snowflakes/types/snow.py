from snovault import (
    abstract_collection,
    calculated_property,
    collection,
    load_schema,
)
from .base import (
    Item,
    paths_filtered_by_status,
)


import datetime


def item_is_revoked(request, path):
    return request.embed(path, '@@object').get('status') == 'revoked'


@abstract_collection(
    name='snowsets',
    unique_key='accession',
    properties={
        'title': "Snowsets",
        'description': 'Abstract class describing different collections of snowflakes.',
    })
class Snowset(Item):
    base_types = ['Snowset'] + Item.base_types
    embedded = [
        'snowflakes',
        'snowflakes.submitted_by',
        'snowflakes.lab',
        'submitted_by',
        'lab',
        'award',
    ]
    audit_inherit = [
        'snowflakes',
        'submitted_by',
        'lab',
        'award',
    ]
    name_key = 'accession'
    rev = {
        'snowflakes': ('Snowflake', 'snowset'),
    }

    @calculated_property(condition='date_released', schema={
        "title": "Month released",
        "type": "string",
    })
    def month_released(self, date_released):
        return datetime.datetime.strptime(date_released, '%Y-%m-%d').strftime('%B, %Y')

    @calculated_property(schema={
        "title": "snowflakes",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkFrom": "Snowflake.snowset",
        },
    })
    def snowflakes(self, request, snowflakes):
        return paths_filtered_by_status(request, snowflakes)


@collection(
    name='snowballs',
    unique_key='accession',
    properties={
        'title': "Snowball style snowset",
        'description': 'A set of snowflakes packed into a snowball.',
    })
class Snowball(Snowset):
    item_type = 'snowball'
    schema = load_schema('snowflakes:schemas/snowball.json')

    @calculated_property(
        schema={
        "title": "test_calculated",
        "type": "string",
        },
        define=True
    )
    def test_calculated(self):
        return 'test_calculated_value'

    @calculated_property(
        schema={
        "title": "another_test_calculated",
        "type": "string",
    })
    def another_test_calculated(self):
        return 'another_test_calculated_value'
    
    @calculated_property(
        schema={
        "title": "conditional_test_calculated",
        "type": "string",
        },
        condition='test_calculated'
    )
    def conditional_test_calculated(self):
        return 'conditional_test_calculated_value'
    
    matrix = {
        'x': {
            'group_by': 'snowflakes.type'
        },
        'y': {
            'group_by': ['award.rfa', 'lab.title']
        }
    }
    
    missing_matrix = {
        'x': {
            'group_by': 'snowflakes.type'
        },
        'y': {
            'group_by': ['award.rfa', ('lab.not_a_real_value', 'some_lab')]
        }
    }
    
    summary_matrix = {
        'x': {
            'group_by': 'status'
        },
        'y': {
            'group_by': ['snowflakes.type']
        }
    }

    audit = {
        'x': {
            'group_by': 'snowflakes.type',
            'label': 'Type',
        },
        'audit.ERROR.category': {
            'group_by': 'audit.ERROR.category',
            'label': 'Error',
        },
        'audit.NOT_COMPLIANT.category': {
            'group_by': 'audit.NOT_COMPLIANT.category',
            'label': 'Not Compliant',
        },
        'audit.WARNING.category': {
            'group_by': 'audit.WARNING.category',
            'label': 'Warning',
        },
        'audit.INTERNAL_ACTION.category': {
            'group_by': 'audit.INTERNAL_ACTION.category',
            'label': 'Internal Action',
        },
    }


@collection(
    name='snowforts',
    unique_key='accession',
    properties={
        'title': "Snowfort style snowset",
        'description': 'A set of snowflakes packed into a snowfort.',
    })
class Snowfort(Snowset):
    item_type = 'snowfort'
    schema = load_schema('snowflakes:schemas/snowfort.json')


@collection(
    name='snowflakes',
    unique_key='accession',
    properties={
        'title': 'Snowflakes',
        'description': 'Listing of Snowflakes',
    })
class Snowflake(Item):
    item_type = 'snowflake'
    schema = load_schema('snowflakes:schemas/snowflake.json')
    name_key = 'accession'

    embedded = [
        'lab',
        'submitted_by',

    ]
    audit_inherit = [
        'lab',
        'submitted_by',

    ]
