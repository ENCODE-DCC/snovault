from functools import wraps
from pyramid.httpexceptions import HTTPBadRequest


def assert_one_or_none(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        if len(r) > 1:
            msg = 'Invalid to specify multiple values: {}'.format(r)
            raise HTTPBadRequest(explanation=msg)
        return r
    return wrapper


def deduplicate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        return list(set(r))
    return wrapper
