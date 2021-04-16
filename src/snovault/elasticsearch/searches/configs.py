from collections.abc import Mapping
from .defaults import DEFAULT_TERMS_AGGREGATION_KWARGS
from .defaults import DEFAULT_EXISTS_AGGREGATION_KWARGS
from .interfaces import SEARCH_CONFIG


def includeme(config):
    registry = config.registry
    registry[SEARCH_CONFIG] = SearchConfigRegistry()


class Config(Mapping):
    '''
    Used for filtering out inappropriate and None **kwargs before passing along to Elasticsearch.
    Implements Mapping type so ** syntax can be used.
    '''

    def __init__(self, allowed_kwargs=[], **kwargs):
        self._allowed_kwargs = allowed_kwargs
        self._kwargs = kwargs

    def _filtered_kwargs(self):
        return {
            k: v
            for k, v in self._kwargs.items()
            if v and k in self._allowed_kwargs
        }

    def __iter__(self):
        return iter(self._filtered_kwargs())

    def __len__(self):
        return len(self._filtered_kwargs())

    def __getitem__(self, key):
        return self._filtered_kwargs()[key]


class TermsAggregationConfig(Config):

    def __init__(self, allowed_kwargs=[], **kwargs):
        super().__init__(
            allowed_kwargs=allowed_kwargs or DEFAULT_TERMS_AGGREGATION_KWARGS,
            **kwargs
        )


class ExistsAggregationConfig(Config):

    def __init__(self, allowed_kwargs=[], **kwargs):
        super().__init__(
            allowed_kwargs=allowed_kwargs or DEFAULT_EXISTS_AGGREGATION_KWARGS,
            **kwargs
        )


def get_search_config():
    return SearchConfig


class SearchConfigRegistry:

    def __init__(self):
        self.registry = {}

    def add(self, config):
        self.registry[config.name] = config

    def register_from_item(self, item):
        config = get_search_config().from_item(item)
        self.add(config)

    def clear(self):
        self.registry = {}

    def get(self, field, default=None):
        return self.registry.get(field, default)


class MutableConfig(Config):

    def update(self, **kwargs):
        self._kwargs.update(kwargs)


class SearchConfig(MutableConfig):

    ITEM_CONFIG_LOCATION = 'schema'
    CONFIG_KEYS = [
        'facets',
        'columns',
        'boost_values',
    ]

    def __init__(self, name, config):
        if config is None:
            config = {}
        super().__init__(
            allowed_kwargs=self.CONFIG_KEYS,
            **config
        )
        self.name = name

    def __getattr__(self, attr):
        value = self.get(attr)
        if value is None:
            raise AttributeError(attr)
        return value

    @classmethod
    def from_item(cls, item):
        return cls(
            item.__name__,
            getattr(
                item,
                cls.ITEM_CONFIG_LOCATION,
                {}
            )
        )
