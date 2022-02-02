# "$schema": "https://json-schema.org/draft/2020-12/schema"
from copy import deepcopy
from jsonschema import Draft202012Validator
from jsonschema import validators
from jsonschema.exceptions import ValidationError
from pyramid.threadlocal import get_current_request
from pyramid.traversal import find_resource


NO_DEFAULT = object()


def normalize_links(links):
    request = get_current_request()
    normalized_links = []
    errors = []
    for link in links:
        try:
            normalized_links.append(
                str(find_resource(request.root, link).uuid)
            )
        except KeyError:
            errors.append(
                ValidationError(f'Unable to resolve link: {link}')
            )
            normalized_links.append(
                link
            )
    return normalized_links, errors


def should_mutate_properties(validator, instance):
    if validator.is_type(instance, 'object'):
        return True
    return False


def get_items_or_empty_object(validator, subschema):
    items = subschema.get('items', {})
    if validator.is_type(items, 'object'):
        return items
    return {}


def maybe_normalize_links_to_uuids(validator, property, subschema, instance):
    errors = []
    if 'linkTo' in subschema:
        link = instance.get(property)
        if link:
            normalized_links, errors = normalize_links([link])
            instance[property] = normalized_links[0]
    if 'linkTo' in get_items_or_empty_object(validator, subschema):
        links = instance.get(property, [])
        if links:
            normalized_links, errors = normalize_links(links)
            instance[property] = normalized_links
    for error in errors:
        yield error


def set_defaults(validator, property, subschema, instance):
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


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS['properties']

    def mutate_properties(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if not validator.is_type(subschema, 'object'):
                continue
            yield from maybe_normalize_links_to_uuids(validator, property, subschema, instance)
            set_defaults(validator, property, subschema, instance)

    def before_properties_validation_hook(validator, properties, instance, schema):
        if should_mutate_properties(validator, instance):
            yield from mutate_properties(validator, properties, instance, schema)
        yield from validate_properties(validator, properties, instance, schema)

    return validators.extend(
        validator_class, {'properties': before_properties_validation_hook},
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
