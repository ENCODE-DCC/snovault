from functools import wraps
from pyramid.httpexceptions import HTTPBadRequest


def assert_condition_returned(condition, error_message=''):
    '''
    Decorator for checking return value of function. Results will
    be passed into condition function and error raised if True.
    '''
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if condition(result):
                msg = '{} {}'.format(error_message, result)
                raise HTTPBadRequest(
                    explanation=msg
                )
            return result
        return wrapper
    return decorator


def assert_none_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: result,
        error_message=error_message
    )


def assert_one_or_none_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: len(result) > 1,
        error_message=error_message
    )


def deduplicate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        return list(set(r))
    return wrapper
