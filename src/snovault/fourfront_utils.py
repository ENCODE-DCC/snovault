# Basic fourfront-specific utilities that seem to have a good home in snovault

def add_default_embeds(item_type, types, embeds, schema={}):
    """Perform default processing on the embeds list.
    This adds display_title and link_id entries to the embeds list for any
    valid paths that do not already include them.
    Also adds display_title and link_id embed paths for any linkTo's in the
    top-level schema that are not already present. This allows for a minimal
    amount of information to be searched on/shown for every field.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    embeds = list(embeds)
    embeds.sort()
    if 'properties' in schema:
        schema = schema['properties']
    processed_fields = embeds[:] if len(embeds) > 0 else []
    already_processed = []
    # find pre-existing fields
    for field in embeds:
        split_field = field.strip().split('.')
        if len(split_field) > 1:
            # ensure that the embed is valid
            is_valid, error_message = confirm_embed_with_schemas(item_type, types, split_field[:-1], schema)
            if not is_valid:
                if error_message:
                    print(error_message)
                continue
            embed_path = '.'.join(split_field[:-1])
            # last part of split_field should a specific fieldname or *
            # if *, then display_title and link_id are taken care of
            if split_field[-1] == '*':
                already_processed.append(embed_path)
                continue
            if embed_path not in already_processed:
                already_processed.append(embed_path)
                if embed_path + '.link_id' not in processed_fields:
                    processed_fields.append(embed_path + '.link_id')
                if embed_path + '.display_title' not in processed_fields:
                    processed_fields.append(embed_path + '.display_title')

    # automatically embed top level linkTo's not already embedded
    schema_default_linkTos = check_for_linkTo('', schema)
    for default_linkTo in schema_default_linkTos:
        # add link_id and display_title for default_linkTo if not already there
        if default_linkTo + '.link_id' not in processed_fields:
            processed_fields.append(default_linkTo + '.link_id')
        if default_linkTo + '.display_title' not in processed_fields:
            processed_fields.append(default_linkTo + '.display_title')
    return processed_fields


def check_for_linkTo(path_thus_far, subschema):
    """
    For a given field and that field's subschema, return the an array of paths
    to the linkTo's in that subschema. Usually, this will just be one linkTo
    (array of length 1). Recursive function.
    """
    linkTo_paths = []
    if subschema.get('type', None) == 'array' and 'items' in subschema:
        items_linkTos = check_for_linkTo(path_thus_far, subschema['items'])
        linkTo_paths += items_linkTos
    if subschema.get('type', None) == 'object' and 'properties' in subschema:
        props_linkTos = check_for_linkTo(path_thus_far, subschema['properties'])
        linkTo_paths += props_linkTos
    for key, val in subschema.items():
        if key == 'items' or key == 'properties':
            continue
        elif key == 'linkTo':
            linkTo_paths.append(path_thus_far)
        elif isinstance(val, dict):
            updated_path = key if path_thus_far == '' else path_thus_far + '.' + key
            item_linkTos = check_for_linkTo(updated_path, val)
            linkTo_paths += item_linkTos
    return linkTo_paths


def confirm_embed_with_schemas(item_type, types, split_path, schema):
    """
    Take an  split embed path without the last term, such as
    [biosample, biosource] (which could have originally been:
    biosample.biosource.*) and confirm that each item in the path has a valid
    schema. Start at the highest level.
    Returns True if valid, False otherwise. If False, it means that your
    embedded list needs to be fixed.
    """
    schema_cursor = schema
    error_message = None
    for idx in range(len(split_path)):
        element = split_path[idx]
        if element in schema_cursor:
            schema_cursor = schema_cursor[element]
            # drill into 'items' or 'properties'
            # always check 'items' before 'properties'
            if schema_cursor.get('type', None) == 'array' and 'items' in schema_cursor:
                schema_cursor = schema_cursor['items']
            if schema_cursor.get('type', None) == 'object' and 'properties' in schema_cursor:
                schema_cursor = schema_cursor['properties']
            # if we hit a linkTo, pull in the new schema of the linkTo type
            if 'linkTo' in schema_cursor:
                linkTo = schema_cursor['linkTo']
                try:
                    linkTo_type = types.all[linkTo]
                except KeyError:
                    error_message = '{} has a bad embed: {} is not a valid type.'.format(item_type, linkTo)
                    return False, error_message
                linkTo_schema = linkTo_type.schema
                schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema
            else:
                # if this is the last element of split_path and there is no
                # linkTo, then the embed is valid but we don't want to add
                # default embedding (don't set the error message)
                if idx == len(split_path) - 1:
                    return False, error_message
        else:
            error_message = '{} has a bad embed: {} is not contained within the parent schema.'.format(item_type, element)
            return False, error_message
    return True, error_message
