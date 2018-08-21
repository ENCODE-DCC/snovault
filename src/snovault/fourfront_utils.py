# Basic fourfront-specific utilities that seem to have a good home in snovault
# Really, are these embed utils? Used with the embedded_list

import sys
from copy import deepcopy

def add_default_embeds(item_type, types, embeds, schema={}):
    """
    Perform default processing on the embeds list.
    Three part process that automatically builds a list of embed paths using
    the embedded_list (embeds parameter), expanding all the top level linkTos,
    and then finally adding the default embeds to all the linkTo paths generated.
    Used in fourfront/../types/base.py AND snovault create mapping
    """
    # remove duplicate embeds
    embeds = list(set(list(embeds)))
    embeds.sort()
    if 'properties' in schema:
        schema = schema['properties']
    processed_embeds = set(embeds[:]) if len(embeds) > 0 else set()
    # add default embeds for items in the embedded_list
    embeds_to_add, processed_embeds = expand_embedded_list(item_type, types, embeds,
                                                           schema, processed_embeds)
    # automatically embed top level linkTo's not already embedded
    # also find subobjects and embed those
    embeds_to_add.extend(find_default_embeds_for_schema('', schema))
    # finally actually add the default embeds
    return build_default_embeds(embeds_to_add, processed_embeds)


def expand_embedded_list(item_type, types, embeds, schema, processed_embeds):
    """
    Takes the embedded_list (as defined in types/ file for an item) and finds
    all items that should have the automatic embeds added to them (namely,
    link_id, display_title, @id, uuid, and principals_allowed).
    """
    embeds_to_add = []
    # Handles the use of a terminal '*' in the embeds
    for embed_path in embeds:
        # ensure that the embed is valid
        split_path = embed_path.strip().split('.')
        error_message, path_embeds_to_add = crawl_schemas_by_embeds(item_type, types, split_path, schema)
        if error_message:
            # remove bad embeds
            # check error_message rather than is_valid because there can
            # be cases of fields that are not valid for default embeds
            # but are still themselves valid fields
            processed_embeds.remove(embed_path)
            print(error_message, file = sys.stderr)
        else:
            embeds_to_add.extend(path_embeds_to_add)
    return embeds_to_add, processed_embeds


def build_default_embeds(embeds_to_add, processed_embeds):
    """
    Actually add the embed path for default embeds using the embeds_to_add
    list generated in add_default_embeds.
    """
    for add_embed in embeds_to_add:
        if add_embed[-2:] == '.*':
            processed_embeds.add(add_embed)
        else:
            # for neatness' sake, ensure redundant embeds are not getting added
            check_wildcard = add_embed + '.*'
            if check_wildcard not in processed_embeds and check_wildcard not in embeds_to_add:
                # default embeds to add
                # link_id can be removed soon
                processed_embeds.add(add_embed + '.@id')
                processed_embeds.add(add_embed + '.link_id')
                processed_embeds.add(add_embed + '.display_title')
                processed_embeds.add(add_embed + '.uuid')
                processed_embeds.add(add_embed + '.principals_allowed.*')
    return list(processed_embeds)


def find_default_embeds_for_schema(path_thus_far, subschema):
    """
    For a given field and that field's subschema, return the an array of paths
    to the objects in that subschema. This includes all linkTo's and any
    subobjects within the subschema. Recursive function.
    """
    linkTo_paths = []
    if subschema.get('type') == 'array' and 'items' in subschema:
        items_linkTos = find_default_embeds_for_schema(path_thus_far, subschema['items'])
        linkTo_paths += items_linkTos
    if subschema.get('type') == 'object' and 'properties' in subschema:
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


def crawl_schemas_by_embeds(item_type, types, split_path, schema):
    """
    Take a split embed_path from the embedded_list and confirm that each item in the
    path has a valid schema. Also return default embeds associated with embed_path.
    If embed_path only has one element, return an error. This is because it is
    a redundant embed (all top level fields and @id/display_title for
    linkTos are added automatically).
    - split_path is embed_path (e.g. biosource.biosample.*) split on '.', so
      ['biosample', 'biosource', '*'] for the example above.
    - types parameter is registry[TYPES].
    A linkTo schema is considered valid if it has @id and display_title fields.
    Return values:
    1. error_message. Either None for no errors or a string to describe the error
    2. embeds_to_add. List of embeds to add for the given embed_path. In the
    case of embed_path ending with a *, this is the default embeds for that
    object's schema. Otherwise, it may just be embed_path, once its validated.
    """
    schema_cursor = schema
    embeds_to_add = []
    error_message = None
    linkTo_path = '.'.join(split_path)
    if len(split_path) == 1:
        error_message = '{} has a bad embed: {} is a top-level field. Did you mean: "{}.*"?.'.format(item_type, split_path[0], split_path[0])
    for idx in range(len(split_path)):
        element = split_path[idx]
        # schema_cursor should always be a dictionary if we have more split_fields
        if not isinstance(schema_cursor, dict):
            error_message = '{} has a bad embed: {} does not have valid schemas throughout.'.format(item_type, linkTo_path)
            return error_message, embeds_to_add
        if element == '*':
            linkTo_path = '.'.join(split_path[:-1])
            if idx != len(split_path) - 1:
                error_message = '{} has a bad embed: * can only be at the end of an embed.'.format(item_type)
            if '@id' in schema_cursor and 'display_title' in schema_cursor:
                # add default linkTos for the '*' object
                embeds_to_add.extend(find_default_embeds_for_schema(linkTo_path, schema_cursor))
            return error_message, embeds_to_add
        elif element in schema_cursor:
            # save prev_schema_cursor in case where last split_path is a non-linkTo field
            prev_schema_cursor = deepcopy(schema_cursor)
            schema_cursor = schema_cursor[element]
            # drill into 'items' or 'properties'. always check 'items' before 'properties'
            # check if an array + drill into if so
            if schema_cursor.get('type', None) == 'array' and 'items' in schema_cursor:
                schema_cursor = schema_cursor['items']
            # check if an object + drill into if so
            if schema_cursor.get('type', None) == 'object' and 'properties' in schema_cursor:
                schema_cursor = schema_cursor['properties']
            # if we hit a linkTo, pull in the new schema of the linkTo type
            # if this is a terminal linkTo, add display_title/@id
            if 'linkTo' in schema_cursor:
                linkTo = schema_cursor['linkTo']
                try:
                    linkTo_type = types.all[linkTo]
                except KeyError:
                    error_message = '{} has a bad embed: {} is not a valid type.'.format(item_type, linkTo)
                    return error_message, embeds_to_add
                linkTo_schema = linkTo_type.schema
                schema_cursor = linkTo_schema['properties'] if 'properties' in linkTo_schema else linkTo_schema
                if '@id' not in schema_cursor or 'display_title' not in schema_cursor:
                    error_message = '{} has a bad embed: {} object does not have @id/display_title.'.format(item_type, linkTo_path)
                    return error_message, embeds_to_add
                # we found a terminal linkTo embed
                if idx == len(split_path) - 1:
                    embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
                else:  # also add default embeds for each intermediate item in the path
                    intermediate_path = '.'.join(split_path[:idx+1])
                    embeds_to_add.append(intermediate_path)
            # not a linkTo. See if this is this is the terminal element
            else:
                # check if this is the last element in path
                if idx == len(split_path) - 1:
                    # in this case, the last element in the embed is a field
                    # remove that from linkTo_path
                    linkTo_path = '.'.join(split_path[:-1])
                    if '@id' in prev_schema_cursor and 'display_title' in prev_schema_cursor:
                        embeds_to_add.append(linkTo_path)
                    return error_message, embeds_to_add
        else:
            error_message = '{} has a bad embed: {} is not contained within the parent schema. See {}.'.format(item_type, element, linkTo_path)
            return error_message, embeds_to_add
    # really shouldn't hit this return, but leave as a back up
    return error_message, embeds_to_add


def get_jsonld_types_from_collection_type(request, doc_type, types_covered=[]):
    """
    Recursively find item types using a given type registry
    Request may also be app (just needs to have registry)
    """
    types_found = []
    try:
        registry_type = request.registry['types'][doc_type]
    except KeyError:
        return [] # no types found
    # add the item_type of this collection if applicable
    if hasattr(registry_type, 'item_type'):
        if registry_type.name not in types_covered:
            types_found.append(registry_type.item_type)
        types_covered.append(registry_type.name)
    # see if we're dealing with an abstract type
    if hasattr(registry_type, 'subtypes'):
        subtypes = registry_type.subtypes
        for subtype in subtypes:
            if subtype not in types_covered:
                types_found.extend(get_jsonld_types_from_collection_type(request, subtype, types_covered))
    return types_found
