from uuid import UUID
from .schema_utils import validate_request, validate, IgnoreUnchanged
from .validation import ValidationFailure


# No-validation validators


def no_validate_item_content_post(context, request):
    data = request.json
    request.validated.update(data)


def no_validate_item_content_put(context, request):
    data = request.json
    if 'uuid' in data:
        if UUID(data['uuid']) != context.uuid:
            msg = 'uuid may not be changed'
            raise ValidationFailure('body', ['uuid'], msg)
    request.validated.update(data)


def no_validate_item_content_patch(context, request):
    data = context.properties.copy()
    data.update(request.json)
    schema = context.type_info.schema
    delete_fields(request, data, schema)
    if 'uuid' in data:
        if UUID(data['uuid']) != context.uuid:
            msg = 'uuid may not be changed'
            raise ValidationFailure('body', ['uuid'], msg)
    request.validated.update(data)


# Delete fields from data in the delete_fields param of the request
# Throw a validation error if the field does not exist within the schema
def delete_fields(request, data, schema):
    if not request.params.get('delete_fields'):
        return

    add_delete_fields(request, data, schema)
    validated, errors = validate(schema, data)
    # don't care about these errors
    errors = [err for err in errors if not isinstance(err, IgnoreUnchanged)]
    if errors:
        for error in errors:
            request.errors.add('body', list(error.path), error.message)
        raise ValidationFailure('body', ['?delete_fields'], 'error deleting fields')

    for dfield in request.params['delete_fields'].split(','):
        dfield = dfield.strip()
        if dfield in data:
            del data[dfield]


def add_delete_fields(request, data, schema):
    if request.params.get('delete_fields'):
        for dfield in request.params['delete_fields'].split(','):
            dfield = dfield.strip()
            val = ''
            field_schema = schema['properties'].get(dfield, {})
            if field_schema.get('linkTo'):
                continue
            if 'default' in field_schema:
                val = field_schema['default']
            elif field_schema.get('type') == 'array':
                val = []
            elif field_schema.get('type') == 'object':
                val = {}
            elif field_schema.get('type') in ['number', 'integer']:
                val = 0
            data[dfield] = val


# Schema checking validators
def validate_item_content_post(context, request):
    data = request.json
    validate_request(context.type_info.schema, request, data)


def validate_item_content_put(context, request):
    data = request.json
    schema = context.type_info.schema
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    current = context.upgrade_properties().copy()
    current['uuid'] = str(context.uuid)
    validate_request(schema, request, data, current)


def validate_item_content_patch(context, request):
    data = context.upgrade_properties().copy()
    if 'schema_version' in data:
        del data['schema_version']
    data.update(request.json)
    schema = context.type_info.schema
    delete_fields(request, data, schema)
    if 'uuid' in data and UUID(data['uuid']) != context.uuid:
        msg = 'uuid may not be changed'
        raise ValidationFailure('body', ['uuid'], msg)
    current = context.upgrade_properties().copy()
    current['uuid'] = str(context.uuid)
    # add deleted fields to current to trigger import-items check
    # i.e. do I have permission to edit, thus delete the field
    # add_delete_fields(request, current, schema)
    validate_request(schema, request, data, current)
    # import pdb; pdb.set_trace()
    # delete_fields(request, request.validated, schema)
