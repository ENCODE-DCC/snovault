from urllib.parse import urlencode
from collections import defaultdict

from .interfaces import ADVANCED_QUERY_KEY
from .interfaces import CART_KEY
from .interfaces import DEBUG_KEY
from .interfaces import FIELD_KEY
from .interfaces import FROM_KEY
from .interfaces import FRAME_KEY
from .interfaces import LIMIT_KEY
from .interfaces import MODE_KEY
from .interfaces import NOT_FLAG
from .interfaces import SEARCH_TERM_KEY
from .interfaces import SORT_KEY
from .interfaces import TYPE_KEY
from .interfaces import WILDCARD


class ParamsParser:
    '''
    Parses query string parameters from request object for use in search.
    '''

    def __init__(self, request):
        self._request = request

    def _params(self, params=None):
        if params is None:
            params = list(self._request.params.items())
        return params

    def is_param(self, key, value, params=None):
        '''
        Returns True if key and value pair exist in params, otherwise False.
        '''
        return (key, value) in self._params(params)

    def get_filters_by_condition(self, key_and_value_condition=None, params=None):
        '''
        Condition must be function that accepts key and value and returns bool. Optional params
        kwarg allows chaining of filters.
        '''
        return [
            (k, v)
            for k, v in self._params(params)
            if key_and_value_condition is None or key_and_value_condition(k, v)
        ]

    def get_key_filters(self, key=None, params=None):
        '''
        Returns params with specified key (= and !=).
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, _: k is None or k == key or k == key + NOT_FLAG,
            params=params
        )

    def get_keys_filters(self, keys=[], params=None):
        '''
        Returns keys contained in keys list (= and !=).
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, _: k in keys or k.replace(NOT_FLAG, '') in keys,
            params=params
        )

    def get_not_keys_filters(self, not_keys=[], params=None):
        '''
        Returns keys not contained in not_keys list (= and !=).
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, _: k not in not_keys and k.replace(NOT_FLAG, '') not in not_keys,
            params=params
        )

    def get_wildcard_filters(self, params=None):
        '''
        Returns params with wildcard value.
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda _, v: v == WILDCARD,
            params=params
        )

    def get_not_wildcard_filters(self, params=None):
        '''
        Returns params without wildcard value.
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda _, v: v != WILDCARD,
            params=params
        )

    def remove_key_and_value_pair_from_filters(self, key=None, value=None, params=None):
        '''
        Returns all params except for the exact key and value pair.
        Used for generating the clear filter links.
        '''
        if key is None or value is None:
            raise ValueError('Must specify key and value')
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, v: not (k == key and v == value),
            params=params
        )

    def get_query_string(self, params=None):
        '''
        Can be called at end of filter chain to return urlencoded string.
        '''
        return urlencode(self._params(params), doseq=True)

    def params_to_list(self, key=False, params=None):
        '''
        Can be called at end of filter chain to return list of keys
        or values. Returns values by default.
        '''
        idx = 0 if key else 1
        return [
            x[idx]
            for x in self._params(params)
        ]

    def param_keys_to_list(self, params=None):
        '''
        Can be called at end of filter chain to return list of keys.
        '''
        return self.params_to_list(key=True, params=params)

    def param_values_to_list(self, params=None):
        '''
        Can be called at end of filter chain to return list of values.
        '''
        return self.params_to_list(key=False, params=params)

    def remove_not_flag(self, params=None, not_flag=NOT_FLAG):
        '''
        Can be used to remove not flag from list of param keys.
        '''
        return [
            (x[0].replace(not_flag, ''), x[1])
            for x in self._params(params)
        ]

    def get_must_match_filters(self, params=None):
        '''
        Returns params where key must equal value.
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, _: not k.endswith(NOT_FLAG),
            params=params
        )

    def get_must_not_match_filters(self, params=None):
        '''
        Returns params where key must not equal value.
        '''
        return self.get_filters_by_condition(
            key_and_value_condition=lambda k, _: k.endswith(NOT_FLAG),
            params=params
        )

    def get_must_filters(self, params=None):
        '''
        Like get_must_match_filters but wildcard values are excluded.
        '''
        return self.get_not_wildcard_filters(
            params=self.get_must_match_filters(
                params=params
            )
        )

    def get_must_not_filters(self, params=None):
        '''
        Like get_must_not_match_filters but wildcard values are excluded.
        '''
        return self.get_not_wildcard_filters(
            params=self.get_must_not_match_filters(
                params=params
            )
        )

    def get_exists_filters(self, params=None):
        '''
        Like get_must_match_filters but only wildcard values are included.
        '''
        return self.get_wildcard_filters(
            params=self.get_must_match_filters(
                params=params
            )
        )

    def get_not_exists_filters(self, params=None):
        '''
        Like get_must_not_match_filters but only wildcard values are included.
        '''
        return self.get_wildcard_filters(
            params=self.get_must_not_match_filters(
                params=params
            )
        )

    def get_type_filters(self, params=None):
        return self.get_key_filters(
            key=TYPE_KEY,
            params=params
        )

    def get_search_term_filters(self, params=None):
        return self.get_key_filters(
            key=SEARCH_TERM_KEY,
            params=params
        )

    def get_must_match_search_term_filters(self, params=None):
        return self.get_must_match_filters(
            params=self.get_search_term_filters(
                params=params
            )
        )

    def get_must_not_match_search_term_filters(self, params=None):
        return self.get_must_not_match_filters(
            params=self.get_search_term_filters(
                params=params
            )
        )

    def get_advanced_query_filters(self, params=None):
        return self.get_key_filters(
            key=ADVANCED_QUERY_KEY,
            params=params
        )

    def get_must_match_advanced_query_filters(self, params=None):
        return self.get_must_match_filters(
            params=self.get_advanced_query_filters(
                params=params
            )
        )

    def get_must_not_match_advanced_query_filters(self, params=None):
        return self.get_must_not_match_filters(
            params=self.get_advanced_query_filters(
                params=params
            )
        )

    def get_field_filters(self, params=None):
        return self.get_key_filters(
            key=FIELD_KEY,
            params=params
        )

    def get_from(self, params=None):
        return self.get_key_filters(
            key=FROM_KEY,
            params=params
        )

    def get_limit(self, params=None):
        return self.get_key_filters(
            key=LIMIT_KEY,
            params=params
        )

    def get_sort(self, params=None):
        return self.get_key_filters(
            key=SORT_KEY,
            params=params
        )

    def get_frame(self, params=None):
        return self.get_key_filters(
            key=FRAME_KEY,
            params=params
        )

    def get_mode(self, params=None):
        return self.get_key_filters(
            key=MODE_KEY,
            params=params
        )

    def get_debug(self, params=None):
        return self.get_key_filters(
            key=DEBUG_KEY,
            params=params
        )

    def get_cart(self, params=None):
        return self.get_key_filters(
            key=CART_KEY,
            params=params
        )

    def group_values_by_key(self, params=None):
        values_by_key = defaultdict(list)
        for p in self._params(params):
            values_by_key[p[0]].append(p[1])
        return dict(values_by_key)

    def get_one_value(self, params=None):
        '''
        Converts param list to values and takes first.
        '''
        values = self.param_values_to_list(
            params=self._params(params)
        )
        if len(values) >= 1:
            return values[0]

    def maybe_int(self, value):
        try:
            return int(value)
        except ValueError:
            return value

    def coerce_value_to_int_or_return_none(self, value):
        value = self.maybe_int(value)
        if isinstance(value, int):
            return value

    def split_filters_by_must_and_exists(self, params=None):
        '''
        Partitions params into four groups: must, must_not, exists, not_exists.
        This is a split based on wildcard and equals/not equals.
        '''
        must = self.get_must_filters(params=params)
        must_not = self.get_must_not_filters(params=params)
        exists = self.get_exists_filters(params=params)
        not_exists = self.get_not_exists_filters(params=params)
        return must, must_not, exists, not_exists


class MutableParamsParser(ParamsParser):
    '''
    Allows for manual modification of query string params and generation of new request.
    '''

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = self._get_original_params()

    def _params(self, params=None):
        if params is None:
            params = self.params
        return params

    def _get_original_params(self):
        return list(self._request.params.items())

    def _get_original_query_string(self):
        return self.get_query_string(
            params=self._get_original_params()
        )

    def _validate_param(self, param):
        conditions = [
            isinstance(param, tuple),
            len(param) == 2
        ]
        if not all(conditions):
            raise ValueError(
                'Param must be a key, value tuple.'
            )

    def _validate_params(self, params):
        if not isinstance(params, list):
            raise ValueError(
                'Params but be a list of key, value tuples.'
            )
        for p in params:
            self._validate_param(p)

    def append(self, param):
        self._validate_param(param)
        self.params.append(param)

    def extend(self, params):
        self._validate_params(params)
        self.params.extend(params)

    def _drop_key(self, key):
        self.params = self.get_not_keys_filters(
            not_keys=[key],
            params=self.params
        )

    def _drop_key_and_value(self, key, value):
        self.params = self.remove_key_and_value_pair_from_filters(
            key=key,
            value=value,
            params=self.params
        )

    def drop(self, param):
        if isinstance(param, tuple):
            self._validate_param(param)
            self._drop_key_and_value(
                key=param[0],
                value=param[1]
            )
        elif isinstance(param, str):
            self._drop_key(param)
        else:
            raise ValueError(
                'Param must be key, value tuple or key string.'
                )

    def clear(self):
        '''
        Clears current params.
        '''
        self.params = []

    def deduplicate(self):
        '''
        Keeps order while deduplicating.
        '''
        params = set()
        params_add = params.add
        self.params = [
            p
            for p in self.params
            if not (p in params or params_add(p))
        ]

    def get_request_with_new_query_string(self, deduplicate=True):
        '''
        Copies request and fills in new deduplicated query_string.
        '''
        if deduplicate:
            self.deduplicate()
        request = self._request.copy()
        request.query_string = self.get_query_string()
        if hasattr(self._request, 'registry'):
            request.registry = self._request.registry
        return request


class QueryString(MutableParamsParser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __repr__(self):
        return self.get_query_string()
