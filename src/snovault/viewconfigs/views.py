from pyramid.view import view_config
from snovault import (		
    AbstractCollection,		
)		
from snovault.resource_views import collection_view_listing_db
from snovault.viewconfigs.searchview import SearchView
from snovault.viewconfigs.report import ReportView


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.scan(__name__)

def iter_search_results(context, request):		
    return search(context, request, return_generator=True)

@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request, search_type=None, return_generator=False, default_doc_types=None, views=None, search_result_actions=None):
    search = SearchView(context, request, search_type, return_generator, default_doc_types)
    return search.preprocess_view(views=views, search_result_actions=search_result_actions)

@view_config(context=AbstractCollection, permission='list', request_method='GET', name='listing')
def collection_view_listing_es(context, request):
    # Switch to change summary page loading options
    if request.datastore != 'elasticsearch':
        return collection_view_listing_db(context, request)

    return search(context, request)