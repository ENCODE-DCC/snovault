# Basic fourfront-specific utilities that seem to have a good home in snovault

import sys

def add_default_embeds(item_type, types, embeds, schema={}):
    """Perform default processing on the embeds list.
    This adds display_title and link_id entries to the embeds list for any
    valid paths that do not already include them.
    Also adds display_title and link_id embed paths for any linkTo's in the
    top-level schema that are not already present. This allows for a minimal
    amount of information to be searched on/shown for every field.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    # remove duplicate embeds
    embeds = list(set(list(embeds)))
    embeds.sort()
    if 'properties' in schema:
        schema = schema['properties']
    processed_fields = embeds[:] if len(embeds) > 0 else []
    already_processed = []
    # find pre-existing fields
    for field in embeds:
        split_field = field.strip().split('.')
        # ensure that the embed is valid
        is_valid, error_message, terminal_linkTo = confirm_embed_with_schemas(item_type, types, split_field, schema)
        if not is_valid:
            if error_message:
                # remove bad embeds
                # check error_message rather than is_valid because there can
                # be cases of fields that are not valid for default embeds
                # but are still themselves valid fields
                a.remove(field)
                print(error_message, file=sys.stderr)
            continue
        if terminal_linkTo:
            embed_path = '.'.join(split_field)
        else:
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
    # also find subobjects and embed those
    schema_default_embeds = find_default_embeds_for_schema('', schema)
    for default_embed in schema_default_embeds:
        # add fully embedded subobjects:
        if default_embed[-2:] == '.*':
            if default_embed not in processed_fields:
                processed_fields.append(default_embed)
            continue
        # add link_id and display_title for default_embed if not already there
        if default_embed + '.link_id' not in processed_fields:
            processed_fields.append(default_embed + '.link_id')
        if default_embed + '.display_title' not in processed_fields:
            processed_fields.append(default_embed + '.display_title')
    return processed_fields


def find_default_embeds_for_schema(path_thus_far, subschema):
    """
    For a given field and that field's subschema, return the an array of paths
    to the linkTo's in that subschema. Usually, this will just be one linkTo
    (array of length 1). Recursive function.
    Additionally, add default embeds to add functionality for automatically
    getting fields in subobjects.
    """
    linkTo_paths = []
    if subschema.get('type', None) == 'array' and 'items' in subschema:
        items_linkTos = find_default_embeds_for_schema(path_thus_far, subschema['items'])
        linkTo_paths += items_linkTos
    if subschema.get('type', None) == 'object' and 'properties' in subschema:
        # we found an object in the schema. embed all its fields
        linkTo_paths.append(path_thus_far + '.*')
        props_linkTos = find_default_embeds_for_schema(path_thus_far, subschema['properties'])
        linkTo_paths += props_linkTos
    for key, val in subschema.items():
        if key == 'items' or key == 'properties':
            continue
        elif key == 'linkTo':
            linkTo_paths.append(path_thus_far)
        elif isinstance(val, dict):
            updated_path = key if path_thus_far == '' else path_thus_far + '.' + key
            item_linkTos = find_default_embeds_for_schema(updated_path, val)
            linkTo_paths += item_linkTos
    return linkTo_paths


def confirm_embed_with_schemas(item_type, types, split_path, schema):
    """
    Take an  split embed path, such a [biosample, biosource, *]
    (which could have originally been:
    biosample.biosource.*) and confirm that each item in the path has a valid
    schema. Start at the highest level.
    If split path only has one element, it is either a top level field/subobjects
    (invalid) or a linked object (valid).
    Return values:
    1. True if valid, False otherwise. If False, it means that your
    embedded list needs to be fixed.
    2. error_message. Either None for no errors or a string to describe the error
    3. terminal_linkTo. Default False, True if the last item in the embedded
    is a linkTo. Behavior is to automatically add display_title and link_id
    in this case.
    """
    schema_cursor = schema
    error_message = None
    for idx in range(len(split_path)):
        element = split_path[idx]
        # schema_cursor should always be a dictionary if we have more split_fields
        if not isinstance(schema_cursor, dict):
            join_path = '.'.join(split_path)
            error_message = '{} has a bad embed: {} does not have valid schemas throughout.'.format(item_type, join_path)
            return False, error_message, False
        if element in schema_cursor or element == '*':
            if element != '*':
                schema_cursor = schema_cursor[element]
            # drill into 'items' or 'properties'
            # always check 'items' before 'properties'
            if schema_cursor.get('type', None) == 'array' and 'items' in schema_cursor:
                schema_cursor = schema_cursor['items']
            if schema_cursor.get('type', None) == 'object' and 'properties' in schema_cursor:
                # test for *, which means there need to be multiple fields present
                # this equates to being and object with 'properties'
                if element == '*':
                    if idx == len(split_path) - 1:
                        return True, error_message, False
                    else:
                        error_message = '{} has a bad embed: * can only be at the end of an embed.'.format(item_type)
                        return False, error_message, False
                schema_cursor = schema_cursor['properties']

            # if we hit a linkTo, pull in the new schema of the linkTo type
            if 'linkTo' in schema_cursor:
                linkTo = schema_cursor['linkTo']
                try:
                    linkTo_type = types.all[linkTo]
                except KeyError:
                    error_message = '{} has a bad embed: {} is not a valid type.'.format(item_type, linkTo)
                    return False, error_message, False
                if idx == len(split_path) - 1:
                    # terminal part of embed is a linkTo (terminal_linkTo)
                    return True, error_message, True
                linkTo_schema = linkTo_type.schema
                schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema
            else:
                # check if last element in path
                if idx == len(split_path) - 1:
                    # if there is only one field (and it's not a linkTo)
                    # this is a invalid top-level field embed
                    if len(split_path == 1):
                        error_message = '{} has a bad embed: {} is a top-level field and should not be embedded.'.format(item_type, element)
                    # if len(split) > 1 but this is still a non-linkTo,
                    # return False because we don't want to add default embeds
                    # but don't set error_message
                    return False, error_message, False
        else:
            error_message = '{} has a bad embed: {} is not contained within the parent schema.'.format(item_type, element)
            return False, error_message, False
    return True, error_message, False
