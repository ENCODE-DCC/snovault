from collections import defaultdict
from functools import lru_cache

from .interfaces import APPENDED
from .interfaces import BUCKETS
from .interfaces import DASH
from .interfaces import DOC_COUNT
from .interfaces import FIELD_KEY
from .interfaces import KEY
from .interfaces import PERIOD
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY


class AggsToFacetsMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facets = []
        self.fake_terms = defaultdict(list)
        self.fake_facets = []

    def _get_total(self):
        return len(self.results)

    def _get_aggregations(self):
        return self.results.aggs.to_dict()

    @lru_cache()
    def _get_facets(self):
        return {
            k: v
            for k, v in self.query_builder._get_facets()
        }

    def _get_facet_name(self, facet_name):
        return facet_name.replace(PERIOD, DASH)

    def _get_facet_title(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TITLE, facet_name)

    def _get_facet_type(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TYPE_KEY, TERMS)

    def _parse_aggregation_bucket_to_list(self, aggregation_bucket):
        '''
        Specifically parses filters aggregations.
        '''
        return [
            {
                KEY: k,
                DOC_COUNT: v.get(DOC_COUNT)
            }
            for k, v in aggregation_bucket.items()
        ]

    def _get_aggregation_result(self, facet_name):
        return self._get_aggregations().get(
            self._get_facet_title(facet_name),
            {}
        )

    def _get_aggregation_bucket(self, facet_name):
        aggregation_bucket = self._get_aggregation_result(
            facet_name
        ).get(self._get_facet_name(facet_name), {}).get(BUCKETS, [])
        if isinstance(aggregation_bucket, dict):
            aggregation_bucket = self._parse_aggregation_bucket_to_list(
                aggregation_bucket
            )
        return aggregation_bucket

    def _get_aggregation_total(self, facet_name):
        return self._get_aggregation_result(
            facet_name
        ).get(DOC_COUNT)

    def _aggregation_is_appended(self, facet_name):
        return self._get_facet_title(facet_name) not in self._get_aggregations()

    def _format_aggregation(self, facet_name):
        facet = {
            FIELD_KEY: facet_name,
            TITLE: self._get_facet_title(facet_name),
            TERMS: self._get_aggregation_bucket(facet_name),
            TOTAL: self._get_aggregation_total(facet_name),
            TYPE_KEY: self._get_facet_type(facet_name),
            APPENDED: self._aggregation_is_appended(facet_name)
        }
        if facet.get(TERMS):
            self.facets.append(facet)

    def _format_aggregations(self):
        self._clear_facets()
        for facet_name in self._get_facets():
            self._format_aggregation(facet_name)

    def to_facets(self):
        self._format_aggregations()
        return self.facets


class HitsToGraphMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_graph(self):
        return [
            r.embedded.to_dict()
            for r in self.results
        ]
