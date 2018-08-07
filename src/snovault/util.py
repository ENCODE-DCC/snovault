from past.builtins import basestring
from pyramid.threadlocal import manager as threadlocal_manager


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
                member = value[index] = request.embed(member, '@@object')
            expand_path(request, member, remaining)
    else:
        if not isinstance(value, dict):
            value = obj[name] = request.embed(value, '@@object')
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


def quick_deepcopy(obj):
    """Deep copy an object consisting of dicts, lists, and primitives.

    This is faster than Python's `copy.deepcopy` because it doesn't
    do bookkeeping to avoid duplicating objects in a cyclic graph.

    This is intended to work fine for data deserialized from JSON,
    but won't work for everything.
    """
    if isinstance(obj, dict):
        obj = {k: quick_deepcopy(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        obj = [quick_deepcopy(v) for v in obj]
    return obj


def mutated_schema(schema, mutator):
    """Apply a change to all levels of a schema.

    Returns a new schema rather than modifying the original.
    """
    schema = mutator(schema.copy())
    if 'items' in schema:
        schema['items'] = mutated_schema(schema['items'], mutator)
    if 'properties' in schema:
        schema['properties'] = schema['properties'].copy()
        for k, v in schema['properties'].items():
            schema['properties'][k] = mutated_schema(v, mutator)
    return schema
