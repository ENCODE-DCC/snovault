"""
# Report View
Some Desc

## Inheritance
ReportView<-SearchView<-BaseView
### SearchView function dependencies
- preprocess_view (Class Method)
### BaseView function dependencies
- _format_facets
"""
from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from snovault.helpers.helper import (
    get_pagination,
    normalize_query
)
from snovault.viewconfigs.searchview import SearchView


class ReportView(SearchView):  # pylint: disable=too-few-public-methods
    '''Report View'''

    def __init__(self, context, request):
        super(ReportView, self).__init__(context, request)
        self._view_name = 'report'
        self._factory_name = None


    def preprocess_view(self, views=None, search_result_actions=None):
        '''
        Main function to construct query and build view results json
        * Only publicly accessible function
        '''
        self._validate_items()
        if ('limit' in self._request.GET and self._request.__parent__ is None
                and (size is None or size > 1000)):
            del self._request.GET['limit']
        # TODO: Fix creating a new instance a SearchView
        # We already do this in __init__
        res = SearchView(self._context, self._request).preprocess_view(
            views=views,
            search_result_actions=search_result_actions,
        )
        view = {
            'href': res['@id'],
            'title': 'View results as list',
            'icon': 'list-alt',
        }
        if not res.get('views'):
            res['views'] = [view]
        else:
            res['views'][0] = view
        search_base = normalize_query(self._request)
        res['@id'] = '/report/' + search_base
        res['title'] = 'Report'
        res['@type'] = ['Report']
        res['non_sortable'] = TEXT_FIELDS
        return res
