import venusian

from snosearch.configs import SearchConfigRegistry
from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG


def includeme(config):
    registry = config.registry
    registry[SEARCH_CONFIG] = SearchConfigRegistry()
    config.add_directive('register_search_config', register_search_config)


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
