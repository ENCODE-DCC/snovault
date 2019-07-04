import pytest

from pyramid.httpexceptions import HTTPBadRequest


def test_searches_decorators_assert_one_or_none():
    from snovault.elasticsearch.searches.decorators import assert_one_or_none
    def dummy_func(values):
        return values
    dummy_func = assert_one_or_none(dummy_func)
    dummy_func([])
    dummy_func([1])
    dummy_func(['one'])
    dummy_func([('one', 'two')])
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
    actual = assert dummy_func(
        [('one', 'two'), ('three', 'two'), ('one', 'two'), ('one', 'one')]
    )
    expected = [('one', 'one'), ('one', 'two'), ('three', 'two')]
    assert len(expected) == len(actual)
    assert all(e in actual for e in expected)
    assert len(set(a) - set(e)) == 0
