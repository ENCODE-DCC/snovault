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
    res = {'error': 'no view permissions'}
    try:
        # if empty item_path reqeust.embed returns just addition as a string
        if item_path:
            res = request.embed(item_path, addition, as_user=True)
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


def select_distinct_values(request, value_path, *from_paths):
    if isinstance(value_path, basestring):
        value_path = value_path.split('.')

    values = from_paths
    for name in value_path:
        objs = (request.embed(member, '@@object') for member in values)
        value_lists = (ensurelist(obj.get(name, [])) for obj in objs)
        values = {value for value_list in value_lists for value in value_list}

    return list(values)
