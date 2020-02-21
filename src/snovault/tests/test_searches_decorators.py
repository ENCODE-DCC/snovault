import pytest

from pyramid.httpexceptions import HTTPBadRequest


def test_searches_decorators_assert_condition_returned():
    from snovault.elasticsearch.searches.decorators import assert_condition_returned
    @assert_condition_returned(condition=lambda x: any(n > 1 for n in x), error_message='Invalid value:')
    def dummy_func(values):
        return values
    assert dummy_func([0, -1, 0, -25]) == [0, -1, 0, -25]
    assert dummy_func([0, 1, 0, -25]) == [0, 1, 0, -25]
    with pytest.raises(HTTPBadRequest) as e:
        dummy_func([0, 2, 0, -25])
    assert str(e.value) == 'Invalid value: [0, 2, 0, -25]'
    assert e.typename == 'HTTPBadRequest'


def test_searches_decorators_assert_none_returned():
    from snovault.elasticsearch.searches.decorators import assert_none_returned
    @assert_none_returned(error_message='Invalid type')
    def dummy_func(values):
        return values
    assert dummy_func([]) == []
    with pytest.raises(HTTPBadRequest):
        dummy_func([1])
    with pytest.raises(HTTPBadRequest):
        dummy_func([1, 2]) == [1, 2]


def test_searches_decorators_assert_one_or_none_returned():
    from snovault.elasticsearch.searches.decorators import assert_one_or_none_returned
    @assert_one_or_none_returned(error_message='Multiple values invalid')
    def dummy_func(values):
        return values
    assert dummy_func([]) == []
    assert dummy_func([1]) == [1]
    assert dummy_func(['one']) == ['one']
    assert dummy_func([('one', 'two')]) == [('one', 'two')]
    with pytest.raises(HTTPBadRequest):
        dummy_func([1, 2])
    with pytest.raises(HTTPBadRequest):
        dummy_func([('one', 'two'), ('one', 'three')])


def test_searches_decorators_assert_one_returned():
    from snovault.elasticsearch.searches.decorators import assert_one_returned
    @assert_one_returned(error_message='Need one value')
    def dummy_func(values):
        return values
    with pytest.raises(HTTPBadRequest):
        dummy_func([]) == []
    assert dummy_func([1]) == [1]
    assert dummy_func(['one']) == ['one']
    assert dummy_func([('one', 'two')]) == [('one', 'two')]
    with pytest.raises(HTTPBadRequest):
        dummy_func([1, 2])
    with pytest.raises(HTTPBadRequest):
        dummy_func([('one', 'two'), ('one', 'three')])


def test_searches_decorators_assert_something_returned():
    from snovault.elasticsearch.searches.decorators import assert_something_returned
    @assert_something_returned(error_message='Nothing returnedd')
    def dummy_func(values):
        return values
    with pytest.raises(HTTPBadRequest):
        dummy_func([])
    assert dummy_func([1]) == [1]
    assert dummy_func(['one']) == ['one']
    assert dummy_func([('one', 'two')]) == [('one', 'two')]
    with pytest.raises(HTTPBadRequest):
        dummy_func({})
    assert dummy_func({'a': 1}) == {'a': 1}


def test_searches_decorators_deduplicate():
    from snovault.elasticsearch.searches.decorators import deduplicate
    def dummy_func(values):
        return values
    dummy_func = deduplicate(dummy_func)
    assert dummy_func([1]) == [1]
    assert dummy_func([]) == []
    assert dummy_func([1]) == [1]
    assert dummy_func([1, 2]) == [1, 2]
    assert dummy_func([1, 2, 1]) == [1, 2]
    assert dummy_func([('one', 'two')]) == [('one', 'two')]
    actual = dummy_func(
        [('one', 'two'), ('three', 'two'), ('one', 'two'), ('one', 'one')]
    )
    expected = [('one', 'one'), ('one', 'two'), ('three', 'two')]
    assert len(expected) == len(actual)
    assert all(e in actual for e in expected)
    assert len(set(actual) - set(expected)) == 0


def test_searces_decorators_remove_from_return():
    from snovault.elasticsearch.searches.decorators import remove_from_return
    @remove_from_return(keys=['del_me'], values=[None])
    def dummy_func(value_dict):
        return value_dict
    assert dummy_func({'a': 1}) == {'a': 1}
    assert dummy_func({'a': 1, 'del_me': 'and me'}) == {'a': 1}
    assert dummy_func({'a': 1, 'b': None, 'del_me': 'and me'}) == {'a': 1}
    assert dummy_func([]) == []
    @remove_from_return(values=[1])
    def dummy_func(value_dict):
        return value_dict
    assert dummy_func({'a': 1}) == {}
    assert dummy_func({'a': 2}) == {'a': 2}


def test_searces_decorators_catch_and_swap():
    from snovault.elasticsearch.searches.decorators import catch_and_swap
    @catch_and_swap()
    def dummy_func():
        raise ValueError
    with pytest.raises(ValueError):
        dummy_func()
    @catch_and_swap(catch=ValueError, swap=TypeError)
    def dummy_func():
        raise ValueError
    with pytest.raises(TypeError):
        dummy_func()
    @catch_and_swap(catch=ValueError, swap=TypeError, details='Invalid type')
    def dummy_func():
        raise ValueError
    with pytest.raises(TypeError) as e:
        dummy_func()
    assert str(e.value) == 'Invalid type'
    @catch_and_swap(catch=TypeError, swap=TypeError, details='Invalid type')
    def dummy_func():
        raise ValueError
    with pytest.raises(ValueError) as e:
        dummy_func()
    @catch_and_swap(catch=ValueError, swap=Exception, details='Invalid type')
    def dummy_func():
        raise ValueError
    with pytest.raises(Exception) as e:
        dummy_func()
