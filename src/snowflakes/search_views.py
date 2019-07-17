from pyramid.view import view_config


from snovault.elasticsearch.searches.interfaces import SEARCH_TITLE
from snovault.elasticsearch.searches.fields import BasicSearchWithFacetsResponseField
from snovault.elasticsearch.searches.fields import RawSearchWithAggsResponseField
from snovault.elasticsearch.searches.fields import TitleResponseField
from snovault.elasticsearch.searches.parsers import ParamsParser
from snovault.elasticsearch.searches.responses import FieldedResponse


def includeme(config):
    config.add_route('searchv2', '/searchv2{slash:/?}')
    config.scan(__name__)


DEFAULT_ITEM_TYPES = [
    'Lab',
    'Snowset',
    'Snowball',
    'Snowfort',
    'Snowflake',
]


@view_config(route_name='searchv2', request_method='GET', permission='search')
def searchv2(context, request):
    fr = FieldedResponse(
        response_fields=[
            TitleResponseField(
                title=SEARCH_TITLE
            ),
            BasicSearchWithFacetsResponseField(
                params_parser=ParamsParser(request),
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='searchv2_raw', request_method='GET', permission='search')
def searchv2_raw(context, request):
    fr = FieldedResponse(
            RawSearchWithAggsResponseField(
                params_parser=ParamsParser(request),
                default_item_types=DEFAULT_ITEM_TYPES
            )
    )
    return fr.render()