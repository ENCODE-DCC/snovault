import venusian

from collections.abc import Mapping
from .defaults import DEFAULT_TERMS_AGGREGATION_KWARGS
from .defaults import DEFAULT_EXISTS_AGGREGATION_KWARGS
from .interfaces import SEARCH_CONFIG


def includeme(config):
    registry = config.registry
    registry[SEARCH_CONFIG] = SearchConfigRegistry()
    config.add_directive('register_search_config', register_search_config)


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


class SortedTupleMap:

    def __init__(self):
        self._map = {}

    @staticmethod
    def _convert_key_to_sorted_tuple(key):
        if isinstance(key, str):
            key = [key]
        return tuple(sorted(key))

    def __setitem__(self, key, value):
        key = self._convert_key_to_sorted_tuple(key)
        self._map[key] = value

    def __getitem__(self, key):
        key = self._convert_key_to_sorted_tuple(key)
        return self._map[key]

    def __contains__(self, key):
        key = self._convert_key_to_sorted_tuple(key)
        return key in self._map

    def get(self, key, default=None):
        return self._map.get(
            self._convert_key_to_sorted_tuple(key),
            default
        )

    def drop(self, key):
        key = self._convert_key_to_sorted_tuple(key)
        if key in self._map:
            del self._map[key]

    def as_dict(self):
        return dict(self._map)


def get_search_config():
    return SearchConfig


class SearchConfigRegistry:

    def __init__(self):
        self._initialize_storage()

    def _initialize_storage(self):
        self.registry = SortedTupleMap()
        self.aliases = SortedTupleMap()
        self.defaults = SortedTupleMap()

    def add(self, config):
        self.registry[config.name] = config

    def add_aliases(self, aliases):
        for k, v in aliases.items():
            self.aliases[k] = v

    def add_defaults(self, defaults):
        for k, v in defaults.items():
            self.defaults[k] = v

    def update(self, config):
        if config.name in self.registry:
            self.get(config.name).update(**config)
        else:
            self.add(config)

    def register_from_func(self, name, func):
        config = get_search_config()(name, func())
        self.update(config)

    def register_from_item(self, item):
        config = get_search_config().from_item(item)
        self.update(config)

    def clear(self):
        self._initialize_storage()

    def get(self, name, default=None):
        return self.registry.get(name, default)

    def _resolve_config_name(self, name, use_defaults=True):
        if name in self.aliases:
            yield from self._resolve_config_names(
                self.aliases[name],
                use_defaults=use_defaults
            )
        elif use_defaults and name in self.defaults:
            yield from self._resolve_config_names(
                self.defaults[name],
                use_defaults=use_defaults
            )
        else:
            yield name

    def _resolve_config_names(self, names, use_defaults=True):
        config_names = []
        for name in names:
            config_names.extend(self._resolve_config_name(name, use_defaults=use_defaults))
        return config_names

    def get_configs_by_names(self, names, use_defaults=True):
        config_names = self._resolve_config_names(names, use_defaults=use_defaults)
        configs = (
            self.get(config_name)
            for config_name in config_names
        )
        return [
            config
            for config in configs
            if config
        ]


class MutableConfig(Config):

    def update(self, **kwargs):
        self._kwargs.update(kwargs)


class SearchConfig(MutableConfig):

    ITEM_CONFIG_LOCATION = 'schema'
    CONFIG_KEYS = [
        'facets',
        'columns',
        'boost_values',
        'matrix',
        'fields',
    ]

    def __init__(self, name, config):
        config = config or {}
        super().__init__(
            allowed_kwargs=self.CONFIG_KEYS,
            **{
                k: v
                for k, v in config.items()
                if k in self.CONFIG_KEYS
            }
        )
        self.name = name

    def __getattr__(self, attr):
        if attr in self.CONFIG_KEYS:
            return self.get(attr, {})
        super().__getattr__(attr)

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


def register_search_config(config, name, factory):
    config.action(
        ('set-search-config', name),
        config.registry[SEARCH_CONFIG].register_from_func,
        args=(
            name,
            factory
        )
    )


def search_config(name, **kwargs):
    '''
    Register a custom search config by name.
    '''
    def decorate(config):
        def callback(scanner, factory_name, factory):
            scanner.config.register_search_config(name, factory)
        venusian.attach(config, callback, category='pyramid')
        return config
    return decorate
