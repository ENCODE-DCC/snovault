from pyramid.view import view_config

from snosearch.interfaces import AUDIT_TITLE
from snosearch.interfaces import MATRIX_TITLE
from snosearch.interfaces import REPORT_TITLE
from snosearch.interfaces import SEARCH_TITLE
from snosearch.interfaces import SUMMARY_MATRIX
from snosearch.interfaces import SUMMARY_TITLE
from snosearch.fields import AuditMatrixWithFacetsResponseField
from snosearch.fields import AllResponseField
from snosearch.fields import BasicMatrixWithFacetsResponseField
from snosearch.fields import BasicSearchResponseField
from snosearch.fields import BasicSearchWithFacetsResponseField
from snosearch.fields import BasicSearchWithoutFacetsResponseField
from snosearch.fields import BasicReportWithFacetsResponseField
from snosearch.fields import BasicReportWithoutFacetsResponseField
from snosearch.fields import CachedFacetsResponseField
from snosearch.fields import ClearFiltersResponseField
from snosearch.fields import ColumnsResponseField
from snosearch.fields import ContextResponseField
from snosearch.fields import DebugQueryResponseField
from snosearch.fields import FiltersResponseField
from snosearch.fields import IDResponseField
from snosearch.fields import MissingMatrixWithFacetsResponseField
from snosearch.fields import NotificationResponseField
from snovault.elasticsearch.searches.fields import NonSortableResponseField
from snosearch.fields import RawMatrixWithAggsResponseField
from snosearch.fields import RawSearchWithAggsResponseField
from snosearch.fields import SearchBaseResponseField
from snosearch.fields import SortResponseField
from snosearch.fields import TitleResponseField
from snosearch.fields import RawTopHitsResponseField
from snosearch.fields import TypeOnlyClearFiltersResponseField
from snosearch.fields import TypeResponseField
from snosearch.parsers import ParamsParser
from snosearch.responses import FieldedGeneratorResponse
from snosearch.responses import FieldedResponse


def includeme(config):
    config.add_route('search', '/search{slash:/?}')
    config.add_route('searchv2_raw', '/searchv2_raw{slash:/?}')
    config.add_route('searchv2_quick', '/searchv2_quick{slash:/?}')
    config.add_route('search-cached-facets', '/search-cached-facets{slash:/?}')
    config.add_route('report', '/report{slash:/?}')
    config.add_route('report-cached-facets', '/report-cached-facets{slash:/?}')
    config.add_route('matrixv2_raw', '/matrixv2_raw{slash:/?}')
    config.add_route('matrix', '/matrix{slash:/?}')
    config.add_route('missing_matrix', '/missing_matrix{slash:/?}')
    config.add_route('summary', '/summary{slash:/?}')
    config.add_route('audit', '/audit{slash:/?}')
    config.add_route('top_hits_raw', '/top_hits_raw{slash:/?}')
    config.scan(__name__)


DEFAULT_ITEM_TYPES = [
    'Lab',
    'Snowset',
    'Snowball',
    'Snowfort',
    'Snowflake',
]


@view_config(route_name='search', request_method='GET', permission='search')
def search(context, request):
    # Note the order of rendering matters for some fields, e.g. AllResponseField and
    # NotificationResponseField depend on results from BasicSearchWithFacetsResponseField.
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=SEARCH_TITLE
            ),
            TypeResponseField(
                at_type=[SEARCH_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            BasicSearchWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            ClearFiltersResponseField(),
            ColumnsResponseField(),
            SortResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='searchv2_raw', request_method='GET', permission='search')
def searchv2_raw(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            RawSearchWithAggsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='searchv2_quick', request_method='GET', permission='search')
def searchv2_quick(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            BasicSearchResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='search-cached-facets', request_method='GET', permission='search')
def search_cached_facets(context, request):
    # Note the order of rendering matters for some fields, e.g. AllResponseField and
    # NotificationResponseField depend on results from BasicSearchWithFacetsResponseField.
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=SEARCH_TITLE
            ),
            TypeResponseField(
                at_type=[SEARCH_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            CachedFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            BasicSearchWithoutFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            ClearFiltersResponseField(),
            ColumnsResponseField(),
            SortResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


def search_generator(request):
    '''
    For internal use (no view). Like search_quick but returns raw generator
    of search hits in @graph field.
    '''
    fgr = FieldedGeneratorResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            BasicSearchResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fgr.render()


@view_config(route_name='report', request_method='GET', permission='search')
def report(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=REPORT_TITLE
            ),
            TypeResponseField(
                at_type=[REPORT_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            BasicReportWithFacetsResponseField(),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            ColumnsResponseField(),
            NonSortableResponseField(),
            SortResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='report-cached-facets', request_method='GET', permission='search')
def report_cached_facets(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=REPORT_TITLE
            ),
            TypeResponseField(
                at_type=[REPORT_TITLE]
            ),
            IDResponseField(),
            ContextResponseField(),
            CachedFacetsResponseField(),
            BasicReportWithoutFacetsResponseField(),
            AllResponseField(),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            ColumnsResponseField(),
            NonSortableResponseField(),
            SortResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='matrixv2_raw', request_method='GET', permission='search')
def matrixv2_raw(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            RawMatrixWithAggsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()


@view_config(route_name='matrix', request_method='GET', permission='search')
def matrix(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=MATRIX_TITLE
            ),
            TypeResponseField(
                at_type=[MATRIX_TITLE]
            ),
            IDResponseField(),
            SearchBaseResponseField(),
            ContextResponseField(),
            BasicMatrixWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='missing_matrix', request_method='GET', permission='search')
def missing_matrix(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=MATRIX_TITLE
            ),
            TypeResponseField(
                at_type=[MATRIX_TITLE]
            ),
            IDResponseField(),
            SearchBaseResponseField(),
            ContextResponseField(),
            MissingMatrixWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES,
                matrix_definition_name='missing_matrix'
            ),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='summary', request_method='GET', permission='search')
def summary(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=SUMMARY_TITLE
            ),
            TypeResponseField(
                at_type=[SUMMARY_TITLE]
            ),
            IDResponseField(),
            SearchBaseResponseField(),
            ContextResponseField(),
            BasicMatrixWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES,
                matrix_definition_name=SUMMARY_MATRIX
            ),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='audit', request_method='GET', permission='search')
def audit(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            TitleResponseField(
                title=AUDIT_TITLE
            ),
            TypeResponseField(
                at_type=[AUDIT_TITLE]
            ),
            IDResponseField(),
            SearchBaseResponseField(),
            ContextResponseField(),
            AuditMatrixWithFacetsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            ),
            NotificationResponseField(),
            FiltersResponseField(),
            TypeOnlyClearFiltersResponseField(),
            DebugQueryResponseField()
        ]
    )
    return fr.render()


@view_config(route_name='top_hits_raw', request_method='GET', permission='search')
def top_hits_raw(context, request):
    fr = FieldedResponse(
        _meta={
            'params_parser': ParamsParser(request)
        },
        response_fields=[
            RawTopHitsResponseField(
                default_item_types=DEFAULT_ITEM_TYPES
            )
        ]
    )
    return fr.render()
