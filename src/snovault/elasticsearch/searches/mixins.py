from .interfaces import APPENDED
from .interfaces import FIELD_KEY
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY


class AggsToFacetsMixin:

    def __init__(self):
        pass

    def _get_aggregation_field(self):
         pass

    def _get_aggregation_title(self):
        pass

    def _get_aggregation_type(self):
        pass

    def _get_aggregation_results(self):
        pass

    def _get_aggregation_total(self):
        pass

    def _aggregation_is_appeneded(self):
        pass

    def _format_aggregation(self):
        return {
            FIELD_KEY: get_aggregation_field(),
            TITLE: get_aggregation_title(),
            TERMS: get_aggregation_results(),
            TOTAL: get_aggregation_total(),
            TYPE_KEY: get_aggregation_type(),
            APPENDED: aggregation_is_appeneded(),
        }


    def _format_aggregations(self):
        pass

    def to_facets(self):
        pass


class HitsToGraphMixin:

    def __init__(self):
        pass

    def to_graph(self):
        return [
            r.embedded.to_dict()
            for r in self.results
        ]
