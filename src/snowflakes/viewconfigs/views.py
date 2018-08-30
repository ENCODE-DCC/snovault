from pyramid.view import view_config
from snovault.viewconfigs.report import ReportView


def includeme(config):
    config.add_route('report', '/report{slash:/?}')
    config.scan(__name__)


@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):

    report = ReportView(context, request)
    return report.preprocess_view()
