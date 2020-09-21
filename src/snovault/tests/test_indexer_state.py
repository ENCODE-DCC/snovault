import pytest


def test_heterogeneous_stream():
    from snovault.elasticsearch.indexer_state import heterogeneous_stream
    gm = {'e': (x for x in [1, 2, 3, 4, 5])}
    assert list(heterogeneous_stream(gm)) == [1, 2, 3, 4, 5]
    gm = {
        'e': (x for x in [1, 2, 3, 4, 5]),
        'f': (x for x in ['a', 'b', 'c'])
    }
    assert list(heterogeneous_stream(gm)) == [1, 'a', 2, 'b', 3, 'c', 4, 5]
    gm = {
        'e': (x for x in [1, 2, 3, 4, 5]),
        'f': (x for x in ['a', 'b', 'c']),
        'g': (x for x in (t for t in (None, True, False, None, None, True)))
    }
    assert list(heterogeneous_stream(gm)) == [
        1, 'a', 2, 'b', True, 3, 'c', False, 4, 5, True
    ]
    gm = {
        'e': [1.0, 2.0, 3.0],
        'f': [x for x in range(10)]
    }
    assert list(heterogeneous_stream(gm)) == [
        1.0, 0, 2.0, 1, 3.0, 2, 3, 4, 5, 6, 7, 8, 9
    ]
