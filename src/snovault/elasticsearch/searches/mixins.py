from collections import defaultdict
from collections import OrderedDict
from functools import lru_cache

from .defaults import AUDIT_FIELDS
from .defaults import KEEP_LAYERED_FIELDS
from .interfaces import APPENDED
from .interfaces import BUCKETS
from .interfaces import DASH
from .interfaces import DOC_COUNT
from .interfaces import FIELD_KEY
from .interfaces import JS_IS_EQUAL
from .interfaces import JS_TRUE
from .interfaces import JS_FALSE
from .interfaces import KEY
from .interfaces import OPEN_ON_LOAD
from .interfaces import PERIOD
from .interfaces import TERMS
from .interfaces import TITLE
from .interfaces import TOTAL
from .interfaces import TYPE_KEY
from .interfaces import X
from .interfaces import Y


class AggsToFacetsMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.facets = []
        self.fake_buckets = defaultdict(list)
        self.fake_facets = []

    @lru_cache()
    def _get_total(self):
        return len(self.results)

    @lru_cache()
    def _get_aggregations(self):
        return self.results.aggs.to_dict()

    @lru_cache()
    def _get_facets(self):
        return OrderedDict(
            self.query_builder._get_facets()
        )

    def _get_post_filters(self):
        return self.query_builder._get_post_filters()

    def _get_facet_name(self, facet_name):
        return facet_name.replace(PERIOD, DASH)

    def _get_facet_title(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TITLE, facet_name)

    def _get_facet_type(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(TYPE_KEY, TERMS)

    def _get_facet_open_on_load(self, facet_name):
        return self._get_facets().get(facet_name, {}).get(OPEN_ON_LOAD, False)

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

    def _get_fake_facets(self):
        facet_keys = [
            k
            for k in self._get_facets()
        ]
        return self.query_builder.params_parser.get_not_keys_filters(
            not_keys=facet_keys,
            params=self._get_post_filters()
        )

    def _aggregation_is_appended(self, facet_name):
        return self._get_facet_title(facet_name) not in self._get_aggregations()

    def _format_aggregation(self, facet_name):
        facet = {
            FIELD_KEY: facet_name,
            TITLE: self._get_facet_title(facet_name),
            TERMS: self._get_aggregation_bucket(facet_name),
            TOTAL: self._get_aggregation_total(facet_name),
            TYPE_KEY: self._get_facet_type(facet_name),
            APPENDED: JS_FALSE,
            OPEN_ON_LOAD: self._get_facet_open_on_load(facet_name),
        }
        if facet.get(TERMS):
            self.facets.append(facet)

    def _format_aggregations(self):
        for facet_name in self._get_facets():
            self._format_aggregation(facet_name)

    def _make_fake_bucket(self, param_key, param_value, is_equal):
        fake_term = {
            KEY: param_value,
            JS_IS_EQUAL: is_equal
        }
        self.fake_buckets[param_key].append(fake_term)

    def _make_fake_buckets(self, params, is_equal):
        for p in params:
            self._make_fake_bucket(
                param_key=p[0],
                param_value=p[1],
                is_equal=is_equal
            )

    def _make_fake_buckets_from_fake_facets(self, fake_facets):
        must, must_not, exists, not_exists = self.query_builder.params_parser.split_filters_by_must_and_exists(
            params=fake_facets
        )
        self._make_fake_buckets(
            params=must + exists,
            is_equal=JS_TRUE
        )
        self._make_fake_buckets(
            params=self.query_builder.params_parser.remove_not_flag(
                params=must_not + not_exists
            ),
            is_equal=JS_FALSE
        )

    def _make_fake_facet(self, facet_name, terms):
        fake_facet = {
            FIELD_KEY: facet_name,
            TITLE: self._get_facet_title(facet_name),
            TERMS: terms,
            APPENDED: JS_TRUE,
            TOTAL: self._get_total()
        }
        self.fake_facets.append(fake_facet)

    def _make_fake_facets(self):
        fake_facets = self._get_fake_facets()
        self._make_fake_buckets_from_fake_facets(fake_facets)
        for facet_name, terms in self.fake_buckets.items():
            self._make_fake_facet(facet_name, terms)

    def to_facets(self):
        self._format_aggregations()
        self._make_fake_facets()
        return self.facets + self.fake_facets


class HitsToGraphMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _limit_generator(self, generator, limit):
        for i, r in enumerate(generator):
            if i >= limit:
                break
            yield r

    def _scan(self):
        results = self.results._search.scan()
        if not self.query_builder._limit_is_all():
            results = self._limit_generator(
                results,
                limit=self.query_builder._get_limit_value_as_int()
            )
        return results

    def _get_results(self):
        if self.query_builder._should_scan_over_results():
            return self._scan()
        return self.results

    def _unlayer(self, hit_dict, keep_layered=KEEP_LAYERED_FIELDS):
        '''
        Removes embedded.* and object.* prefix from results but keeps audit.* prefix.
        '''
        r = {}
        for k, v in hit_dict.items():
            if k in keep_layered:
                r.update({k: v})
            else:
                r.update(v)
        return OrderedDict(sorted(r.items()))

    def to_graph(self):
        return (
            self._unlayer(r.to_dict())
            for r in self._get_results()
        )


class RawHitsToGraphMixin(HitsToGraphMixin):
    '''
    Like HitsToGraphMixin but renders raw hit.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def to_graph(self):
        return [
            r.to_dict()
            for r in self._get_results()
        ]


class AggsToMatrixMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.matrix = {}

    @lru_cache()
    def _get_aggregations(self):
        return self.results.aggs.to_dict()

    def _add_matrix_definition_to_matrix(self):
        self.matrix.update(
            self.query_builder._get_matrix_for_item_type(
                self.query_builder.params_parser.get_one_value(
                    self.query_builder._get_item_types()
                )
            ).copy()
        )

    def _add_agg_to_matrix(self, key, agg):
        self.matrix.get(key, {}).update(agg)

    def _add_x_agg_to_matrix(self):
        self._add_agg_to_matrix(
            X,
            self._get_aggregations().get(X, {})
        )

    def _add_y_agg_to_matrix(self):
        self._add_agg_to_matrix(
            Y,
            self._get_aggregations().get(Y, {})
        )

    def _build_matrix(self):
        self._add_matrix_definition_to_matrix()
        self._add_x_agg_to_matrix()
        self._add_y_agg_to_matrix()

    def to_matrix(self):
        self._build_matrix()
        return self.matrix


class AuditAggsToMatrixMixin(AggsToMatrixMixin):

    def _add_audit_aggs_to_matrix(self):
        for field in AUDIT_FIELDS:
            self._add_agg_to_matrix(
                field,
                self._get_aggregations().get(field, {})
            )

    def _build_matrix(self):
        self._add_matrix_definition_to_matrix()
        self._add_x_agg_to_matrix()
        self._add_audit_aggs_to_matrix()
