import pytest

from collections import OrderedDict

from jsonschema_serialize_fork.validators import Draft4Validator
from jsonschema_serialize_fork import NO_DEFAULT
from jsonschema_serialize_fork.exceptions import ValidationError


validator_class = Draft4Validator


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
