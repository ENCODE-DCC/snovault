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
    assert tac._allowed_kwargs == ['size', 'exclude', 'missing']
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
