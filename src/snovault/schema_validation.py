# Draft202012Validator
# "$schema": "https://json-schema.org/draft/2020-12/schema"
from jsonschema import Draft202012Validator
from jsonschema import validators
from copy import deepcopy


NO_DEFAULT = object()


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS['properties']

    def should_set_defaults(validator, instance):
        if validator.is_type(instance, 'object'):
            return True
        return False

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if not validator.is_type(subschema, 'object'):
                continue
            if 'default' in subschema:
                instance.setdefault(
                    property,
                    deepcopy(subschema['default'])
                )
            if 'serverDefault' in subschema:
                server_default = validator.server_default(
                    instance,
                    subschema
                )
                if server_default is not NO_DEFAULT:
                    instance.setdefault(
                        property,
                        server_default
                    )

    def properties_with_defaults(validator, properties, instance, schema):
        if should_set_defaults(validator, instance):
            set_defaults(validator, properties, instance, schema)
        yield from validate_properties(validator, properties, instance, schema)

    return validators.extend(
        validator_class, {'properties': properties_with_defaults},
    )


ExtendedValidator = extend_with_default(Draft202012Validator)


class SerializingSchemaValidator(ExtendedValidator):

    SERVER_DEFAULTS = {}

    def add_server_defaults(self, server_defaults):
        self.SERVER_DEFAULTS.update(server_defaults)
        return self

    def serialize(self, instance):
        self._original_instance = instance
        self._mutated_instance = deepcopy(
            self._original_instance
        )
        errors = list(
            self.iter_errors(
                self._mutated_instance
            )
        )
        return self._mutated_instance, errors

    def server_default(self, instance, subschema):
        factory_name = subschema['serverDefault']
        factory = self.SERVER_DEFAULTS[factory_name]
        return factory(instance, subschema)
