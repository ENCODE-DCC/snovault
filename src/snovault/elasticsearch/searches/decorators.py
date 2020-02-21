from functools import wraps
from pyramid.httpexceptions import HTTPBadRequest


def assert_condition_returned(condition, error_message='', exception=HTTPBadRequest):
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
                raise exception(
                    explanation=msg
                )
            return result
        return wrapper
    return decorator


def assert_none_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: bool(result),
        error_message=error_message
    )


def assert_one_or_none_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: len(result) > 1,
        error_message=error_message
    )


def assert_one_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: len(result) != 1,
        error_message=error_message
    )


def assert_something_returned(error_message):
    return assert_condition_returned(
        condition=lambda result: len(result) == 0,
        error_message=error_message
    )


def deduplicate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        r = func(*args, **kwargs)
        return list(set(r))
    return wrapper


def remove_from_return(keys=[], values=[]):
    '''
    Removes dict item if it matches key or value.
    '''
    def remove_from_return_decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            r = func(*args, **kwargs)
            if isinstance(r, dict):
                keys_to_remove = []
                for k, v in r.items():
                    if k in keys or v in values:
                        keys_to_remove.append(k)
                # Avoid mutating while iterating over.
                for k in keys_to_remove:
                    r.pop(k, None)
            return r
        return wrapper
    return remove_from_return_decorator


def catch_and_swap(catch=Exception, swap=None, details=None):
    '''
    Catch given exception and raise new exception instead.
    '''
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                result = func(*args, **kwargs)
            except catch as e:
                if not swap:
                    raise e
                raise swap(details)
            else:
                return result
        return wrapper
    return decorator

