from past.builtins import basestring
from pyramid.threadlocal import manager as threadlocal_manager
from pyramid.httpexceptions import HTTPForbidden


def includeme(config):
    config.add_request_method(select_distinct_values)


def get_root_request():
    if threadlocal_manager.stack:
        return threadlocal_manager.stack[0]['request']


def ensurelist(value):
    if isinstance(value, basestring):
        return [value]
    return value


def simple_path_ids(obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        yield obj
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if not isinstance(value, list):
        value = [value]
    for member in value:
        for result in simple_path_ids(member, remaining):
            yield result


def secure_embed(request, item_path, addition='@@object'):
    """
    Make a call to embed() with the given item path and user status
    Handles substituting a no view permissions message if a the given
    request does not have permission to see the object
    """
    res = {'error': 'no view permissions'}
    try:
        # if empty item_path reqeust.embed returns just addition as a string
        if item_path:
            res = request.embed(str(item_path), addition, as_user=True)
        else:
            res = ''
        return res
    except HTTPForbidden:
        print("you don't have access to this object")

    return res


def expand_path(request, obj, path):
    if isinstance(path, basestring):
        path = path.split('.')
    if not path:
        return
    name = path[0]
    remaining = path[1:]
    value = obj.get(name, None)
    if value is None:
        return
    if isinstance(value, list):
        for index, member in enumerate(value):
            if not isinstance(member, dict):
                res = secure_embed(request, member, '@@object')
                member = value[index] = res
            expand_path(request, member, remaining)
    else:
        if not isinstance(value, dict):
            res = secure_embed(request, value, '@@object')
            value = obj[name] = res
        expand_path(request, value, remaining)


def expand_embedded_model(request, obj, model):
    """
    A similar idea to expand_path, but takes in a model from build_embedded_model
    instead. Takes in the @@object view of the item (obj) and returns a
    fully embedded result.
    """
    embedded_res = {}
    # first take care of the fields_to_use at this level
    fields_to_use = model.get('fields_to_use')
    if fields_to_use:
        if '*' in fields_to_use:
            embedded_res = obj
        else:
            for field in fields_to_use:
                found = obj.get(field)
                if found:
                    embedded_res[field] = found
    # then handle objects at the next level
    for to_embed in model:
        if to_embed == 'fields_to_use':
            continue
        obj_val = obj.get(to_embed)
        if obj_val is None:
            continue
        obj_embedded = expand_val_for_embedded_model(request, obj_val, model[to_embed])
        if obj_embedded is not None:
            embedded_res[to_embed] = obj_embedded
    return embedded_res


def expand_val_for_embedded_model(request, obj_val, downstream_model):
    """
    Take a value from an object and the relevant piece of the embedded_model
    and perform embedding.
    We have to account for list, dictionaries, and strings.
    """
    #
    if isinstance(obj_val, list):
        obj_list = []
        for member in obj_val:
            obj_embedded = expand_val_for_embedded_model(request, member, downstream_model)
            if obj_embedded is not None:
                obj_list.append(obj_embedded)
        return obj_list
    elif isinstance(obj_val, dict):
        obj_embedded = expand_embedded_model(request, obj_val, downstream_model)
        return obj_embedded
    elif isinstance(obj_val, basestring):
        # get the @@object view of obj to embed
        obj_val = secure_embed(request, obj_val, '@@object')
        if not obj_val or obj_val == {'error': 'no view permissions'}:
            return obj_val
        obj_embedded = expand_embedded_model(request, obj_val, downstream_model)
        return obj_embedded
    else:
        # this means the object should be returned as-is
        return obj_val


def build_embedded_model(fields_to_embed):
    """
    Takes a list of fields to embed and builds the framework used to generate
    the fully embedded result. 'fields_to_use' refer to specific fields that are to
    be embedded within an object. The base level object gets a special flag,
    '*', which means all non-object fields are embedded by default.
    Below is an example calculated from the following fields:
    INPUT:
    [modifications.modified_regions.chromosome,
    lab.uuid,
    award.*,
    biosource.name]
    OUTPUT:
    {'modifications': {'modified_regions': {'fields_to_use': ['chromosome']}},
     'lab': {'fields_to_use': ['uuid']},
     'award': {'fields_to_use': ['*']},
     'bisource': {'fields_to_use': ['name']},
     'fields_to_use': ['*']}
    """
    embedded_model = {'fields_to_use':['*']}
    for field in fields_to_embed:
        split_field = field.split('.')
        field_pointer = embedded_model
        for subfield in split_field:
            if subfield == split_field[-1]:
                if 'fields_to_use' in field_pointer:
                    field_pointer['fields_to_use'].append(subfield)
                else:
                    field_pointer['fields_to_use'] = [subfield]
                continue
            elif subfield not in field_pointer:
                field_pointer[subfield] = {}
            field_pointer = field_pointer[subfield]
    return embedded_model


def select_distinct_values(request, value_path, *from_paths):
    if isinstance(value_path, basestring):
        value_path = value_path.split('.')

    values = from_paths
    for name in value_path:
        objs = (request.embed(member, '@@object') for member in values)
        value_lists = (ensurelist(obj.get(name, [])) for obj in objs)
        values = {value for value_list in value_lists for value in value_list}

    return list(values)
