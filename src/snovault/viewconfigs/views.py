from pyramid.view import view_config
from snovault.viewconfigs.searchview import SearchView
from snovault.viewconfigs.report import ReportView


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('report', '/report{slash:/?}')
    config.scan(__name__)


@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False):

    search = SearchView(context, request, search_type, return_generator)
    return search.preprocess_view()


@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):

    report = ReportView(context, request)
    return report.preprocess_view()
