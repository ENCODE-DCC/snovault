# Basic fourfront-specific utilities that seem to have a good home in snovault

import sys
from copy import deepcopy

def add_default_embeds(item_type, types, embeds, schema={}):
    """Perform default processing on the embeds list.
    This adds display_title and link_id entries to the embeds list for any
    valid paths that do not already include them.
    Also adds display_title and link_id embed paths for any linkTo's in the
    top-level schema that are not already present. This allows for a minimal
    amount of information to be searched on/shown for every field.
    Lastly, for any embed with a .* at the end, add display_title and link_id
    for all linked objects at that level, as well as all fields at that level.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    # remove duplicate embeds
    embeds = list(set(list(embeds)))
    embeds.sort()
    if 'properties' in schema:
        schema = schema['properties']
    processed_fields = embeds[:] if len(embeds) > 0 else []
    processed_fields = set(processed_fields)
    embeds_to_add = []
    # First, verify existing embeds (defined in types file) and add link_id and
    # display_title to those paths if needed
    # Handles the use of a terminal '*' in the embeds
    for field in embeds:
        split_field = field.strip().split('.')
        # ensure that the embed is valid
        error_message, field_embeds_to_add = confirm_embed_with_schemas(item_type, types, split_field, schema)
        if error_message:
            # remove bad embeds
            # check error_message rather than is_valid because there can
            # be cases of fields that are not valid for default embeds
            # but are still themselves valid fields
            processed_fields.remove(field)
            print(error_message, file=sys.stderr)
        else:
            embeds_to_add.extend(field_embeds_to_add)

    # automatically embed top level linkTo's not already embedded
    # also find subobjects and embed those
    embeds_to_add.extend(find_default_embeds_for_schema('', schema))
    for add_embed in embeds_to_add:
        if add_embed[-2:] == '.*':
            processed_fields.add(add_embed)
        else:
            # for neatness' sake, ensure redundant embeds are getting added
            check_wildcard = add_embed + '.*'
            if check_wildcard not in processed_fields and check_wildcard not in embeds_to_add:
                processed_fields.add(add_embed + '.link_id')
                processed_fields.add(add_embed + '.display_title')
    return list(processed_fields)


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
    1. error_message. Either None for no errors or a string to describe the error
    2. embeds_to_add. Since link_id and display_title are added automatically
    for all linked objects, return these for any valid object embed. Also, in
    the case '*' is used, add default embeds at the relative level.
    """
    schema_cursor = schema
    embeds_to_add = []
    error_message = None
    linkTo_path = '.'.join(split_path)
    if len(split_path) == 1:
        error_message = '{} has a bad embed: {} is a top-level field and should not be embedded.'.format(item_type, split_path[0])
    for idx in range(len(split_path)):
        element = split_path[idx]
        # schema_cursor should always be a dictionary if we have more split_fields
        if not isinstance(schema_cursor, dict):
            error_message = '{} has a bad embed: {} does not have valid schemas throughout.'.format(item_type, linkTo_path)
            return error_message, embeds_to_add
        # if this a valid field in the current schema_mapping
        if element in schema_cursor or element == '*':
            # save prev_schema_cursor in case where last split_path is a non-linkTo field
            if element != '*':
                prev_schema_cursor = deepcopy(schema_cursor)
                schema_cursor = schema_cursor[element]
            else:
                # handle *
                linkTo_path = '.'.join(split_path[:-1])
                if idx != len(split_path) - 1:
                    error_message = '{} has a bad embed: * can only be at the end of an embed.'.format(item_type)
                if 'link_id' in schema_cursor and 'display_title' in schema_cursor:
                    # add default linkTos for the '*' object
                    embeds_to_add.extend(find_default_embeds_for_schema(linkTo_path, schema_cursor))
                return error_message, embeds_to_add

            # drill into 'items' or 'properties'
            # always check 'items' before 'properties'
            if schema_cursor.get('type', None) == 'array' and 'items' in schema_cursor:
                schema_cursor = schema_cursor['items']
            if schema_cursor.get('type', None) == 'object' and 'properties' in schema_cursor:
                # test for *, which means there need to be multiple fields present
                # this equates to being and object with 'properties'
                schema_cursor = schema_cursor['properties']

            # if we hit a linkTo, pull in the new schema of the linkTo type
            if 'linkTo' in schema_cursor:
                linkTo = schema_cursor['linkTo']
                try:
                    linkTo_type = types.all[linkTo]
                except KeyError:
                    error_message = '{} has a bad embed: {} is not a valid type.'.format(item_type, linkTo)
                    return error_message, embeds_to_add
                # we found a terminal linkTo embed
                if idx == len(split_path) - 1:
                    if 'link_id' not in schema_cursor or 'display_title' not in schema_cursor:
                        error_message = '{} has a bad embed: {}; terminal object does not have link_id/display_title.'.format(item_type, linkTo_path)
                    else:
                        embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
                linkTo_schema = linkTo_type.schema
                schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema
            else: # not a linkTo
                # check if this is the last element in path
                if idx == len(split_path) - 1:
                    # in this case, the last element in the embed is a field
                    # remove that from linkTo_path
                    linkTo_path = '.'.join(split_path[:-1])
                    if 'link_id' in prev_schema_cursor and 'display_title' in prev_schema_cursor:
                        embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
        else:
            error_message = '{} has a bad embed: {} is not contained within the parent schema. See {}.'.format(item_type, element, linkTo_path)
            return error_message, embeds_to_add

    # really shouldn't hit this return, but leave as a back up
    return error_message, embeds_to_add
