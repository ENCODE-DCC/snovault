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
import time
import logging
import datetime
from pyramid.httpexceptions import HTTPBadRequest  # pylint: disable=import-error

from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from snovault.helpers.helper import (
    get_pagination,
    normalize_query
)
from snovault.viewconfigs.searchview import SearchView


class ReportView(SearchView):  # pylint: disable=too-few-public-methods
    '''Report View'''
    view_name = 'report'
    _factory_name = None
    def __init__(self, context, request):
        super(ReportView, self).__init__(context, request)
        logging.basicConfig(filename='search_test_1.log', format='%(asctime)s %(message)s' ,level=logging.DEBUG)
        super(SearchView, self).__init__(context, request)
        self._search_type = search_type
        self._return_generator = return_generator
        self._default_doc_types = default_doc_types or []
        self._context = context

        # Create the Logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
 
        # Create the Handler for logging data to a file
        logger_handler = logging.FileHandler('search_test_1.log')
        logger_handler.setLevel(logging.DEBUG)
 
        # Create a Formatter for formatting the log messages
        logger_formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
 
        # Add the Formatter to the Handler
        logger_handler.setFormatter(logger_formatter)
 
        # Add the Handler to the Logger
        self.logger.addHandler(logger_handler)

    def preprocess_view(self, views=None, search_result_actions=None):
        '''
        Main function to construct query and build view results json
        * Only publicly accessible function
        '''
        t_start = time.time()
        doc_types = self._request.params.getall('type')
        if len(doc_types) != 1:
            msg = 'Report view requires specifying a single type.'
            raise HTTPBadRequest(explanation=msg)
        try:
            sub_types = self._types[doc_types[0]].subtypes
        except KeyError:
            msg = "Invalid type: " + doc_types[0]
            raise HTTPBadRequest(explanation=msg)
        if len(sub_types) > 1:
            msg = 'Report view requires a type with no child types.'
            raise HTTPBadRequest(explanation=msg)
        t_end = time.time()
        self.logger.debug('{} time: {:.20f}'.format(
            'SNOVAULT Report preprocess_view _a_',
            (t_end - t_start)*1000
        ))
        _, size = get_pagination(self._request)
        t_end = time.time()
        self.logger.debug('{} time: {:.20f}'.format(
            'SNOVAULT Report preprocess_view _b_',
            (t_end - t_start)*1000
        ))
        if ('limit' in self._request.GET and self._request.__parent__ is None
                and (size is None or size > 1000)):
            del self._request.GET['limit']
        # TODO: Fix creating a new instance a SearchView
        # We already do this in __init__
        res = SearchView(self._context, self._request).preprocess_view(
            views=views,
            search_result_actions=search_result_actions,
        )
        t_end = time.time()
        self.logger.debug('{} time: {:.20f}'.format(
            'SNOVAULT Report preprocess_view _c_',
            (t_end - t_start)*1000
        ))
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
        t_end = time.time()
        self.logger.debug('{} time: {:.20f}'.format(
            'SNOVAULT Report preprocess_view',
            (t_end - t_start)*1000
        ))
        return res
