import pytest

from pyramid.httpexceptions import HTTPBadRequest


def test_searches_decorators_assert_condition_returned():
    assert False


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
