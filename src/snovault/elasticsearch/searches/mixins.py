from .interfaces import APPENDED
from .interfaces import BUCKET
from .interfaces import FIELD_KEY
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY


class AggsToFacetsMixin:

    def __init__(self):
        self.facets = []

    def _get_aggregations(self):
        return self.results.aggs.to_dict()

    def _get_facets(self):
        return {
            k: v
            for k, v in self.query_builder._get_facets()
        }

    def _get_facet_title(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TITLE)

    def _get_facet_type(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TYPE_KEY, TERMS)

    def _get_aggregation_results(self, facet_name):
        return self._get_aggregations().get(
            self._get_facet_title(facet_name),
            {}
        ).get(BUCKET)

    def _get_aggregation_total(self):
        return self.results.hits.total

    def _aggregation_is_appeneded(self, facet_name):
        return self._get_facet_title(facet_name) not in self._get_aggregations()

    def _format_aggregation(self, facet_name):
        self.facets.append(
            {
                FIELD_KEY: facet_name,
                TITLE: self._get_facet_title(facet_name),
                TERMS: self._get_aggregation_results(facet_name),
                TOTAL: self._get_aggregation_total(),
                TYPE_KEY: self._get_facet_type(facet_name),
                APPENDED: self._aggregation_is_appeneded(facet_name),
            }
        )

    def _format_aggregations(self):
        self.facets = []
        for facet_name in self._get_facets():
            self._format_aggregation(facet_name)

    def to_facets(self):
        self._format_aggregations()
        return self.facets


class HitsToGraphMixin:

    def __init__(self):
        pass

    def to_graph(self):
        return [
            r.embedded.to_dict()
            for r in self.results
        ]
