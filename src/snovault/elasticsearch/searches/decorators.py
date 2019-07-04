from functools import wraps
from pyramid.httpexceptions import HTTPBadRequest


def assert_one_or_none(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        values = func(*args, **kwargs)
        if len(values) > 1:
            msg = 'Invalid to specify multiple values: {}'.format(values)
            raise HTTPBadRequest(explanation=msg)
        return values
    return wrapper


def deduplicate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs):
        return list(set(result))
