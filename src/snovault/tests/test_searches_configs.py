import pytest


def included(config):
    def new_item_search_config():
        return {
            'facets': {'a': 'b'}
        }
    config.register_search_config(
        'OtherConfigItem', new_item_search_config
    )
    

def test_searches_configs_search_config_decorator(config, dummy_request):
    from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG
    from snovault.elasticsearch.searches.configs import search_config
    assert dummy_request.registry[SEARCH_CONFIG].get('TestConfigItem').facets == {'a': 'b'}
    config.include('snovault.elasticsearch.searches.configs')
    config.include(included)
    config.commit()
    assert config.registry[SEARCH_CONFIG].registry.get('OtherConfigItem').facets == {'a': 'b'}
    config.register_search_config('OtherConfigItem', lambda: {'facets': {'c': 'd'}})
    config.commit()
    assert config.registry[SEARCH_CONFIG].registry.get('OtherConfigItem').facets == {'c': 'd'}
