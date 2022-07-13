CORS_RESPONSE_HEADERS = [
    'Access-Control-Allow-Origin',
    'Access-Control-Allow-Methods',
    'Access-Control-Allow-Headers',
    'Access-Control-Expose-Headers',
    'Access-Control-Allow-Credentials',
    'Access-Control-Max-Age',
    'Vary',
]


def get_cors_headers(request):
    return {
        k: v
        for k, v in request.response.headers.items()
        if k in CORS_RESPONSE_HEADERS
    }
