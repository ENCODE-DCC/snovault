from snovault.viewconfigs.searchview import SearchView
from pyramid.httpexceptions import HTTPBadRequest
from snovault.elasticsearch.create_mapping import TEXT_FIELDS
from snovault.helpers.helper import (
    get_pagination, 
    normalize_query
)


class ReportView(SearchView):
    def __init__(self, context, request):
        super(ReportView, self).__init__(context, request)

    def preprocess_view(self):
        doc_types = self.request.params.getall('type')
        
        if len(doc_types) != 1:
            msg = 'Report view requires specifying a single type.'
            raise HTTPBadRequest(explanation=msg)
    
        # Get the subtypes of the requested type
        try:
            sub_types = self.types[doc_types[0]].subtypes
        except KeyError:
            # Raise an error for an invalid type
            msg = "Invalid type: " + doc_types[0]
            raise HTTPBadRequest(explanation=msg)

        # Raise an error if the requested type has subtypes.
        if len(sub_types) > 1:
            msg = 'Report view requires a type with no child types.'
            raise HTTPBadRequest(explanation=msg)

        # Ignore large limits, which make `search` return a Response
        # -- UNLESS we're being embedded by the download_report view
        from_, size = get_pagination(self.request)
        if ('limit' in self.request.GET and self.request.__parent__ is None
                and (size is None or size > 1000)):
            del self.request.GET['limit']
        # Reuse search view
        res = SearchView(self.context, self.request).preprocess_view()
        
        # change @id, @type, and views
        res['views'][0] = {
            'href': res['@id'],
            'title': 'View results as list',
            'icon': 'list-alt',
        }
        search_base = normalize_query(self.request)
        res['@id'] = '/report/' + search_base
        #res['download_tsv'] = self.request.route_path('report_download') + search_base
        res['title'] = 'Report'
        res['@type'] = ['Report']
        res['non_sortable'] = TEXT_FIELDS
        return res
