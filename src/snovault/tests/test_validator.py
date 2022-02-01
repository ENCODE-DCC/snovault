import pytest

from collections import OrderedDict

from jsonschema_serialize_fork.validators import Draft4Validator
from jsonschema_serialize_fork import NO_DEFAULT
from jsonschema_serialize_fork.exceptions import ValidationError


validator_class = Draft4Validator


fake_schema = {
    'title': 'Fake',
    'description': 'Schema',
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'type': 'object',
    'required': ['award', 'lab'],
    'identifyingProperties': ['uuid'],
    'additionalProperties': False,
    'properties': {
        'uuid': {
            'type': 'string'
        },
        'award': {
            'type': 'string'
        },
        'lab': {
            'type': 'string'
        }
    }
}


def make_default(instance, subschema):
    if instance.get('skip'):
        return NO_DEFAULT
    assert subschema == {'serverDefault': 'test'}
    return 'bar'


def test_validator_serializes_default_properties():
    schema = {
        'properties': {
            'foo': {
                'default': 'bar'
            }
        }
    }
    result, errors = validator_class(
        schema,
        serialize=True
    ).serialize(
        {}
    )
    assert result == {'foo': 'bar'}
    assert errors == []


def test_validator_serializes_default_properties_in_items():
    schema = {
        'items': {
            'properties': {
                'foo': {
                    'default': 'bar'
                }
            }
        }
    }
    result, errors = validator_class(
        schema,
        serialize=True
    ).serialize(
        [
            {}
        ]
    )
    assert result == [{'foo': 'bar'}]
    assert errors == []


def test_validator_serializes_server_default_properties():
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            }
        }
    }
    result, errors = validator_class(
        schema,
        serialize=True,
        server_defaults={
            'test': make_default
        },
    ).serialize(
        {}
    )
    assert result == {'foo': 'bar'}
    assert errors == []


def test_validator_ignores_server_default_returning_no_default():
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            },
            'skip': {}
        }
    }
    result, errors = validator_class(
        schema,
        serialize=True,
        server_defaults={
            'test': make_default
        },
    ).serialize(
        {
            'skip': True
        }
    )
    assert result == {'skip': True}
    assert errors == []


def test_validator_serializes_server_default_properties_in_items():
    schema = {
        'items': {
            'properties': {
                'foo': {
                    'serverDefault': 'test'
                }
            }
        }
    }
    result, errors = validator_class(
        schema,
        serialize=True,
        server_defaults={
            'test': make_default
        },
    ).serialize(
        [
            {}
        ]
    )
    assert result == [{'foo': 'bar'}]
    assert errors == []


def test_validator_serializes_properties_in_order():
    schema = {
        'properties': OrderedDict(
            [
                ('foo', {}),
                ('bar', {})
            ]
        )
    }
    validator = validator_class(
        schema,
        types={'object': OrderedDict},
        serialize=True,
    )
    value = OrderedDict(
        [
            ('bar', 1),
            ('foo', 2)
        ]
    )
    result, errors = validator.serialize(value)
    assert isinstance(result, OrderedDict)
    assert list(result) == ['foo', 'bar']
    assert errors == []


def test_validator_serializes_properties_in_order_with_dict():
    schema = {
        'properties': OrderedDict(
            [
                ('foo', {}),
                ('bar', {})
            ]
        )
    }
    validator = validator_class(
        schema,
        types={
            'object': (OrderedDict, dict)
        },
        serialize=True,
    )
    value = dict(
        [
            ('bar', 1),
            ('foo', 2)
        ]
    )
    result, errors = validator.serialize(value)
    assert isinstance(result, OrderedDict)
    assert list(result) == ['foo', 'bar']
    assert errors == []


def test_validator_returns_error():
    schema = {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = validator_class(
        schema,
        serialize=True
    ).serialize(
        {
            'name': 'abc'
        }
    )
    assert result == {'name': 'abc'}
    assert not errors
    result, errors = validator_class(
        schema,
        serialize=True
    ).serialize(
        {
            'name': 1
        }
    )
    assert result == {
        'name': 1
    }
    assert isinstance(errors[0], ValidationError)
    assert errors[0].message == "1 is not of type 'string'"
    result, errors = validator_class(
        schema,
        serialize=True
    ).serialize(
        {}
    )
    assert result == {}
    assert isinstance(errors[0], ValidationError)
    assert errors[0].message == "'name' is a required property"


def test_validator_check_schema():
    from jsonschema import Draft202012Validator
    validator_class.check_schema(fake_schema)
    Draft202012Validator.check_schema(fake_schema)


def test_validator_extend_with_default():
    from copy import deepcopy
    from snovault.schema_validation import SerializingSchemaValidator
    original_instance = {
        'x': 'y'
    }
    mutated_instance = deepcopy(original_instance)
    assert original_instance == mutated_instance
    schema = {'properties': {'foo': {'default': 'bar'}}}
    SerializingSchemaValidator(schema).validate(mutated_instance)
    assert original_instance == {'x': 'y'}
    assert mutated_instance == {'x': 'y', 'foo': 'bar'}


def test_validator_extend_with_default_and_serialize():
    instance = {
        'x': 'y'
    }
    from snovault.schema_validation import SerializingSchemaValidator
    schema = {'properties': {'foo': {'default': 'bar'}}}
    result, errors = SerializingSchemaValidator(schema).serialize(instance)
    assert instance == {'x': 'y'}
    assert result == {'x': 'y', 'foo': 'bar'}
    assert errors == []
    schema = {
        'properties': {
            'foo': {
                'default': 'bar'
            },
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = SerializingSchemaValidator(schema).serialize(
        {
            'foo': 'thing',
        }
    )
    assert result == {'foo': 'thing'}
    assert errors[0].message == "'name' is a required property"


def test_validator_extend_with_server_default_and_serialize():
    instance = {
        'x': 'y'
    }
    from snovault.schema_validation import SerializingSchemaValidator
    SerializingSchemaValidator.SERVER_DEFAULTS = {
        'test': make_default
    }
    schema = {'properties': {'foo': {'serverDefault': 'test'}}}
    result, errors = SerializingSchemaValidator(schema).serialize(instance)
    assert instance == {'x': 'y'}
    assert result == {'x': 'y', 'foo': 'bar'}
    assert errors == []
    schema = {
        'properties': {
            'foo': {
                'serverDefault': 'test'
            },
            'name': {
                'type': 'string'
            }
        },
        'required': ['name']
    }
    result, errors = SerializingSchemaValidator(schema).serialize(
        {
            'foo': 'thing',
        }
    )
    assert result == {'foo': 'thing'}
    assert errors[0].message == "'name' is a required property"
    result, errors = SerializingSchemaValidator(schema).serialize(
        {
            'name': 'other thing',
        }
    )
    assert result == {
        'foo': 'bar',
        'name': 'other thing',
    }
    assert not errors
