from pyramid.view import view_config

from snovault import AbstractCollection
from snovault.resource_views import collection_view_listing_db
from snovault.viewconfigs.report import ReportView
from snovault.viewconfigs.searchview import SearchView


def includeme(config):
    config.add_route('searchv1', '/searchv1{slash:/?}')
    config.add_route('reportv1', '/reportv1{slash:/?}')
    config.scan(__name__)


DEFAULT_DOC_TYPES = [
    'Lab',
    'Snowset',
    'Snowball',
    'Snowfort',
    'Snowflake',
]


@view_config(context=AbstractCollection, permission='list', request_method='GET', name='listing')
def collection_view_listing_es(context, request):
    # Switch to change summary page loading options
    if request.datastore != 'elasticsearch':
        return collection_view_listing_db(context, request)
    return searchv1(context, request)


@view_config(route_name='searchv1', request_method='GET', permission='search')
def searchv1(context, request, search_type=None, views=None, return_generator=False, search_result_actions=None):
    search = SearchView(context, request, search_type, return_generator, DEFAULT_DOC_TYPES)
    return search.preprocess_view(views=views, search_result_actions=search_result_actions)


@view_config(route_name='report', request_method='GET', permission='search')
def reportv1(context, request):
    report = ReportView(context, request)
    return report.preprocess_view(views=[])
