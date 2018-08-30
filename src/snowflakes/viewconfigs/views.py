from pyramid.view import view_config
from snovault.viewconfigs.searchview import SearchView
from snovault.viewconfigs.report import ReportView


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('report', '/report{slash:/?}')
    config.scan(__name__)


def iter_search_results(context, request):
    return search(context, request, return_generator=True)


@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False):

    default_doc_types = [
        'Lab',
        'Snowset',
        'Snowball',
        'Snowfort',
        'Snowflake',
    ]

    search = SearchView(context, request, search_type, return_generator, default_doc_types)

    return search.preprocess_view()


@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):

    report = ReportView(context, request)
    return report.preprocess_view()
