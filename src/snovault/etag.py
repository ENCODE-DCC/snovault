from pyramid.httpexceptions import (
    HTTPNotModified,
    HTTPPreconditionFailed,
)


def etag_app_version(view_callable):
    def wrapped(context, request):
        etag = request.registry.settings['snovault.app_version']
        if etag in request.if_none_match:
            raise HTTPNotModified()
        result = view_callable(context, request)
        request.response.etag = etag
        return result

    return wrapped


def etag_app_version_effective_principals(view_callable):
    def wrapped(context, request):
        app_version = request.registry.settings['snovault.app_version']
        etag = app_version + ' ' + ' '.join(sorted(request.effective_principals))
        if etag in request.if_none_match:
            raise HTTPNotModified()
        result = view_callable(context, request)
        request.response.etag = etag
        cache_control = request.response.cache_control
        cache_control.private = True
        cache_control.max_age = 0
        cache_control.must_revalidate = True
        return result

    return wrapped
