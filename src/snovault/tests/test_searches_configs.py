import pytest


def test_searches_configs_config_init():
    from snovault.elasticsearch.searches.configs import Config
    c = Config()
    assert isinstance(c, Config)


def test_searches_configs_terms_aggregation_config_init():
    from snovault.elasticsearch.searches.configs import TermsAggregationConfig
    tac = TermsAggregationConfig()
    assert isinstance(tac, TermsAggregationConfig)


def test_searches_configs_exists_aggregation_config_init():
    from snovault.elasticsearch.searches.configs import ExistsAggregationConfig
    eac = ExistsAggregationConfig()
    assert isinstance(eac, ExistsAggregationConfig)


def test_searches_configs_config_allowed_kwargs():
    from snovault.elasticsearch.searches.configs import Config
    c = Config()
    assert c._allowed_kwargs == []
    c = Config(allowed_kwargs=['size'])
    assert c._allowed_kwargs == ['size']


def test_searches_configs_config_kwargs():
    from snovault.elasticsearch.searches.configs import Config
    c = Config()
    assert c._kwargs == {}
    c = Config(other_thing='abc', something_else=True)
    assert c._kwargs == {'other_thing': 'abc', 'something_else': True}


def test_searches_configs_config_allowed_kwargs_and_kwargs():
    from snovault.elasticsearch.searches.configs import Config
    c = Config(
        allowed_kwargs=['first_thing'],
        other_thing='abc',
        something_else=True
    )
    assert c._allowed_kwargs == ['first_thing']
    assert c._kwargs == {'other_thing': 'abc', 'something_else': True}


def test_searches_configs_config_filtered_kwargs():
    from snovault.elasticsearch.searches.configs import Config
    c = Config()
    assert c._filtered_kwargs() == {}
    c = Config(
        allowed_kwargs=['first_thing'],
        other_thing='abc',
        something_else=True
    )
    assert c._filtered_kwargs() == {}
    c = Config(
        allowed_kwargs=['first_thing', 'other_thing'],
        other_thing='abc',
        something_else=True
    )
    assert c._filtered_kwargs() == {'other_thing': 'abc'}
    c = Config(
        allowed_kwargs=['first_thing', 'other_thing'],
        other_thing=None,
        something_else=True
    )
    assert c._filtered_kwargs() == {}


def test_searches_configs_config_iter():
    from snovault.elasticsearch.searches.configs import Config
    c = Config(
        allowed_kwargs=['first_thing', 'other_thing'],
        other_thing='abc',
        something_else=True
    )
    assert {k: v for k, v in c.items()} == {'other_thing': 'abc'}


def test_searches_configs_config_len():
    from snovault.elasticsearch.searches.configs import Config
    c = Config(
        allowed_kwargs=['first_thing', 'other_thing'],
        other_thing='abc',
        something_else=True
    )
    assert len(c) == 1


def test_searches_configs_config_getitem():
    from snovault.elasticsearch.searches.configs import Config
    c = Config(
        allowed_kwargs=['first_thing', 'other_thing'],
        other_thing='abc',
        something_else=True
    )
    assert c['other_thing'] == 'abc'
    with pytest.raises(KeyError):
        c['nothing']


def test_searches_configs_terms_aggregation_config_allowed_kwargs():
    from snovault.elasticsearch.searches.configs import TermsAggregationConfig
    from snovault.elasticsearch.searches.defaults import DEFAULT_TERMS_AGGREGATION_KWARGS
    tac = TermsAggregationConfig()
    assert tac._allowed_kwargs == ['size', 'exclude', 'missing', 'include', 'aggs']
    tac = TermsAggregationConfig(allowed_kwargs=['size'])
    assert tac._allowed_kwargs == ['size']


def test_searches_configs_exists_aggregatin_config_allowed_kwargs():
    from snovault.elasticsearch.searches.configs import ExistsAggregationConfig
    from snovault.elasticsearch.searches.defaults import DEFAULT_TERMS_AGGREGATION_KWARGS
    eac = ExistsAggregationConfig()
    assert eac._allowed_kwargs == []
    eac = ExistsAggregationConfig(allowed_kwargs=['size'])
    assert eac._allowed_kwargs == ['size']


def test_searches_configs_terms_aggregation_config_pass_filtered_kwargs():
    from snovault.elasticsearch.searches.configs import TermsAggregationConfig
    def return_kwargs(**kwargs):
        return kwargs
    kwargs = return_kwargs(**TermsAggregationConfig({}))
    assert kwargs == {}
    kwargs = return_kwargs(**TermsAggregationConfig(size=100))
    assert kwargs == {'size': 100}
    kwargs = return_kwargs(**TermsAggregationConfig(**{'size': 100}))
    assert kwargs == {'size': 100}
    kwargs = return_kwargs(
         **TermsAggregationConfig(
             size=100,
             exclude=None,
             missing='fake'
         )
     )
    assert kwargs == {'size': 100, 'missing': 'fake'}


def test_searches_configs_exists_aggregation_config_pass_filtered_kwargs():
    from snovault.elasticsearch.searches.configs import ExistsAggregationConfig
    def return_kwargs(**kwargs):
        return kwargs
    kwargs = return_kwargs(**ExistsAggregationConfig({}))
    assert kwargs == {}
    e = ExistsAggregationConfig(size=100)
    kwargs = return_kwargs(**ExistsAggregationConfig(size=100))
    assert kwargs == {}
    kwargs = return_kwargs(
         **ExistsAggregationConfig(
             size=100,
             exclude=None,
             missing='fake'
         )
     )
    assert kwargs == {}


def test_searches_configs_sorted_tuple_map_init(dummy_request):
    from snovault.elasticsearch.searches.configs import SortedTupleMap
    s = SortedTupleMap()
    assert isinstance(s, SortedTupleMap)


def test_searches_configs_sorted_tuple_map_convert_key_to_sorted_tuple(dummy_request):
    from snovault.elasticsearch.searches.configs import SortedTupleMap
    s = SortedTupleMap()
    assert s._convert_key_to_sorted_tuple('Experiment') == ('Experiment',)
    assert s._convert_key_to_sorted_tuple(['File' ,'Experiment']) == ('Experiment', 'File')
    assert s._convert_key_to_sorted_tuple(('File' ,'Experiment')) == ('Experiment', 'File')


def test_searches_configs_sorted_tuple_map_add_drop_get_and_as_dict(dummy_request):
    from snovault.elasticsearch.searches.configs import SortedTupleMap
    s = SortedTupleMap()
    s['x'] = ['y']
    assert s.as_dict() == {('x',): ['y']}
    s['x'].extend(['z', 'p'])
    assert s.as_dict() == {('x',): ['y', 'z', 'p']}
    s.drop('x')
    assert s.as_dict() == {}
    s.drop(('x',))
    assert s.as_dict() == {}
    s[['File', 'Experiment', 'QualityMetric']] = ['FileConfig', 'OtherConfig']
    assert s.get(('File', 'Experiment', 'QualityMetric')) == ['FileConfig', 'OtherConfig']
    s[['Experiment', 'QualityMetric', 'File']].extend([{'x', 'y'}])
    assert s.get(('File', 'Experiment', 'QualityMetric')) == ['FileConfig', 'OtherConfig', {'x', 'y'}]
    s.drop(['Experiment', 'QualityMetric', 'File'])
    assert s.get(('File', 'Experiment', 'QualityMetric')) is None
    assert s.get(('File', 'Experiment', 'QualityMetric'), default={}) == {}


def test_searches_configs_sorted_tuple_map_contains(dummy_request):
    from snovault.elasticsearch.searches.configs import SortedTupleMap
    s = SortedTupleMap()
    assert 'x' not in s
    s['x'] = [1, 2, 3]
    assert 'x' in s
    assert ('x',) in s
    assert ['x'] in s
    assert ['x', 'y', 'z'] not in s
    s[('y', 'z', 'x')] = 'abc'
    assert ['x', 'y', 'z'] in s


def test_searches_configs_search_config_registry(dummy_request):
    from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG
    config = dummy_request.registry[SEARCH_CONFIG].get('TestingSearchSchema')
    assert list(config.facets.items()) == [
        ('status', {'title': 'Status', 'open_on_load': True}),
        ('name', {'title': 'Name'})
    ]
    assert list(config.boost_values.items()) == [
        ('accession', 1.0), ('status', 1.0),
        ('label', 1.0)
    ]
    assert list(config.facets.items()) == [
        ('status', {'title': 'Status', 'open_on_load': True}),
        ('name', {'title': 'Name'})
    ]
    original_kwargs = config._kwargs
    config._kwargs = original_kwargs.copy()
    with pytest.raises(AttributeError):
        config.fake_field
    config._allowed_kwargs.append('fake_field')
    config.update(fake_field={'x': 1, 'y': [1, 2]})
    assert config.fake_field == {'x': 1, 'y': [1, 2]}
    config.update(**{'facets': {'new': 'values'}})
    assert config.facets == {'new': 'values'}
    config._kwargs = original_kwargs


def test_searches_configs_search_config_registry_add_aliases_and_defaults(dummy_request):
    from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG
    search_registry = dummy_request.registry[SEARCH_CONFIG]
    config = search_registry.get('TestingSearchSchema')
    assert list(config.facets.items()) == [
        ('status', {'title': 'Status', 'open_on_load': True}),
        ('name', {'title': 'Name'})
    ]
    aliases = {
        'SomeAlias': ['AliasesItem1', 'AliasesItem2']
    }
    defaults = {
        'SomeItem': ['DefaultConfig']
    }
    search_registry.add_aliases(aliases)
    assert search_registry.aliases.as_dict() == {
        ('SomeAlias',): ['AliasesItem1', 'AliasesItem2']
    }
    search_registry.add_defaults(defaults)
    assert search_registry.defaults.as_dict() == {
        ('SomeItem',): ['DefaultConfig']
    }
    search_registry.add_aliases({('AnotherAlias', 'Multkey', 'AndSorted'): ['XYZ']})
    assert search_registry.aliases.as_dict() == {
        ('AndSorted', 'AnotherAlias', 'Multkey'): ['XYZ'],
        ('SomeAlias',): ['AliasesItem1', 'AliasesItem2']
    }


def test_searches_configs_search_config_registry_resolve_config_names(dummy_request):
    from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG
    search_registry = dummy_request.registry[SEARCH_CONFIG]
    registry = search_registry.registry
    config_names = search_registry._resolve_config_names(['TestingSearchSchema'])
    assert len(config_names) == 1
    assert config_names == ['TestingSearchSchema']
    search_registry.add_aliases(
        {
            'AllConfigs': ['TestingSearchSchema']
        }
    )
    config_names = search_registry._resolve_config_names(['AllConfigs'])
    assert len(config_names) == 1
    assert config_names == ['TestingSearchSchema']
    search_registry.add_aliases(
        {
            'AllConfigs': ['TestingSearchSchema', 'TestingSearchSchema']
        }
    )
    config_names = search_registry._resolve_config_names(['AllConfigs'])
    assert len(config_names) == 2
    assert config_names == ['TestingSearchSchema', 'TestingSearchSchema']
    search_registry.add_defaults(
        {
            'TestingSearchSchema': ['SomeOtherConfig']
        }
    )
    config_names = search_registry._resolve_config_names(['AllConfigs'])
    assert len(config_names) == 2
    assert config_names == ['SomeOtherConfig', 'SomeOtherConfig']
    config_names = search_registry._resolve_config_names(['AllConfigs'], use_defaults=False)
    assert len(config_names) == 2
    assert config_names == ['TestingSearchSchema', 'TestingSearchSchema']
    search_registry.clear()
    search_registry.registry = registry


def test_searches_configs_search_config_registry_get_configs_by_names(dummy_request):
    from snovault.elasticsearch.searches.interfaces import SEARCH_CONFIG
    search_registry = dummy_request.registry[SEARCH_CONFIG]
    configs = search_registry.get_configs_by_names(['TestingSearchSchema'])
    assert len(configs) == 1
    assert list(configs[0].facets.items()) == [
        ('status', {'title': 'Status', 'open_on_load': True}),
        ('name', {'title': 'Name'})
    ]

def test_searches_configs_search_config_can_update():
    from snovault.elasticsearch.searches.configs import SearchConfig
    from snovault.elasticsearch.searches.configs import SearchConfigRegistry
    registry = SearchConfigRegistry()
    config = SearchConfig(
        'my-custom-config',
        {
            'facets': {'a': 'b'},
            'columns': ['x', 'y']
        }
    )
    assert list(config.items()) == [
        ('facets', {'a': 'b'}),
        ('columns', ['x', 'y'])
    ]
    registry.add(config)
    empty = SearchConfig('empty', {})
    assert registry.get('my-custom-config', empty).facets == {'a': 'b'}
    assert registry.get('my-custom-config', empty).columns == ['x', 'y']
    assert registry.get('my-custom-config', empty).boost_values == {}
    related_config = SearchConfig(
        'my-custom-config',
        {
            'facets': {'c': 'd'},
            'boost_values': {'t': 'z'}
        }
    )
    registry.update(related_config)
    assert registry.get('my-custom-config', empty).facets == {'c': 'd'}
    assert registry.get('my-custom-config', empty).columns == ['x', 'y']
    assert registry.get('my-custom-config', empty).boost_values == {'t': 'z'}


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
