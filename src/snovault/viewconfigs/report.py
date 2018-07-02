from snovault.viewconfigs.searchview import SearchView
from pyramid.httpexceptions import HTTPBadRequest
from snovault.elasticsearch.create_mapping import TEXT_FIELDS


class ReportView(SearchView):
    def __init__(self, context, request):
        super(ReportView, self).__init__(context, request)

    def preprocess_view(self):
        if len(self.doc_types) != 1:
            msg = 'Report view requires specifying a single type.'
            raise HTTPBadRequest(explanation=msg)
        try:
            sub_types = self.types[self.doc_types[0]].subtypes
        except KeyError:
            # Raise an error for an invalid type
            msg = "Invalid type: " + self.doc_types[0]
            raise HTTPBadRequest(explanation=msg)

        # Raise an error if the requested type has subtypes.
        if len(sub_types) > 1:
            msg = 'Report view requires a type with no child types.'
            raise HTTPBadRequest(explanation=msg)
        if ('limit' in self.request.GET and self.request.__parent__ is None and (self.size is None or self.size > 1000)):
            del self.request.GET['limit']

        super(ReportView, self).preprocess_view()

        self.result['views'][0] = {
            'href': self.result['@id'],
            'title': 'View results as list',
            'icon': 'list-alt',
        }
        self.result['@id'] = '/report/' + self.search_base
        self.result['download_tsv'] = self.request.route_path('report_download') + self.search_base
        self.result['title'] = 'Report'
        self.result['@type'] = ['Report']
        self.result['non_sortable'] = TEXT_FIELDS

        return self.result
