import pytest


from snovault.util import _get_calculated_properties_from_paths
from snovault.util import select_distinct_values


# Test item with calculated property.
COLLECTION_URL = '/testing-link-targets/'


def test_get_calculated_properties_from_paths(dummy_request):
    paths = [
        COLLECTION_URL + 'ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert 'reverse' in calculated_properties


def test_not_a_collection_get_calculated_properties_from_paths(dummy_request):
    paths = [
        '/yxa/ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert not calculated_properties


def test_unformed_path_get_calculated_properties_from_paths(dummy_request):
    paths = [
        'testing-link-targets/ABC123/'
    ]
    calculated_properties = _get_calculated_properties_from_paths(dummy_request, paths)
    assert not calculated_properties


def test_select_distinct_values_returns_calculated(dummy_request, threadlocals, posted_targets_and_sources):
    distinct_values = select_distinct_values(dummy_request, 'reverse', *['/testing-link-targets/one/'])
    assert '/testing-link-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/' in distinct_values


def test_select_distinct_values_uses_calculated(dummy_request, threadlocals, posted_targets_and_sources, mocker):
    mocker.patch.object(dummy_request, 'embed')
    select_distinct_values(dummy_request, 'reverse', *['/testing-link-targets/one/'])
    dummy_request.embed.assert_called_with('/testing-link-targets/one/', '@@object')


def test_select_distinct_values_skips_calculated(dummy_request, threadlocals, posted_targets_and_sources, mocker):
    mocker.patch.object(dummy_request, 'embed')
    select_distinct_values(dummy_request, 'name', *['/testing-link-targets/one/'])
    dummy_request.embed.assert_called_with('/testing-link-targets/one/', '@@object?skip_calculated=true')


def test_types_utils_ensure_list():
    from snovault.util import ensure_list_and_filter_none
    assert ensure_list_and_filter_none('abc') == ['abc']
    assert ensure_list_and_filter_none(['abc']) == ['abc']
    assert ensure_list_and_filter_none({'a': 'b'}) == [{'a': 'b'}]
    assert ensure_list_and_filter_none([{'a': 'b'}, {'c': 'd'}]) == [{'a': 'b'}, {'c': 'd'}]
    assert ensure_list_and_filter_none([{'a': 'b'}, {'c': 'd'}, None]) == [{'a': 'b'}, {'c': 'd'}]


def test_types_utils_take_one_or_return_none():
    from snovault.util import take_one_or_return_none
    assert take_one_or_return_none(['just one']) == 'just one'
    assert take_one_or_return_none(['one', 'and', 'two']) is None
    assert take_one_or_return_none('just one') is None


def test_util_path_init():
    from snovault.util import Path
    p = Path('abc')
    assert isinstance(p, Path)
    assert p.path == 'abc'
    assert p._frame == '@@object'
    assert p._params == {
        'include': [],
        'exclude': []
    }
    assert p.frame == '@@object'


def test_util_path_include_and_exclude_and_frame():
    from snovault.util import Path
    p = Path('/snoflakes/SNS123ABC/', include=['@id', '@type', 'uuid'])
    assert p.frame == '@@filtered_object?include=%40id&include=%40type&include=uuid'
    assert p.path == '/snoflakes/SNS123ABC/'
    p = Path('/snoflakes/SNS123ABC/', exclude=['description'])
    assert p.frame == '@@filtered_object?exclude=description'
    p = Path('/snoflakes/SNS123ABC/', include=['title', 'name'], exclude=['description', 'notes'])
    assert p.frame == '@@filtered_object?include=title&include=name&exclude=description&exclude=notes'
    p = Path(
        '/snoflakes/SNS123ABC/',
        frame='@@embedded',
        include=['title', 'name'],
        exclude=['description', 'notes']
    )
    assert p.frame == '@@filtered_object?include=title&include=name&exclude=description&exclude=notes'
    p = Path(
        '/snoflakes/SNS123ABC/',
        frame='@@embedded',
    )
    assert p.frame == '@@embedded'

def test_util_path_build_frame():
    from snovault.util import Path
    p = Path('/snoflakes/SNS123ABC/', include=['@id', '@type', 'uuid'])
    p._params = {'field': ['a', 'b', 'c']}
    assert p._build_frame() == '@@filtered_object?field=a&field=b&field=c'


def test_util_path_expand_path_with_frame(dummy_request, threadlocals, posted_custom_embed_targets_and_sources):
    from snovault.util import Path
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets')
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties['targets'][0] == {
        'name': 'one',
        '@id': '/testing-custom-embed-targets/one/',
        '@type': ['TestingCustomEmbedTarget', 'Item'],
        'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f',
        'reverse': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'filtered_reverse': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'filtered_reverse1': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'reverse_uncalculated': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/']
    }
    p = Path('targets.reverse')
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties['targets'][0]['reverse'][0] == {
        'name': 'A',
        'status': 'current',
        'target': '/testing-custom-embed-targets/one/',
        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/',
        '@type': ['TestingCustomEmbedSource', 'Item'], 'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd'
    }
    p = Path('targets.reverse.target', include=['name', 'uuid'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties['targets'][0]['reverse'][0]['target'] == {
        'name': 'one',
        'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'
    }


def test_util_path_expand_path_with_frame_include(dummy_request, threadlocals, posted_custom_embed_targets_and_sources):
    from snovault.util import Path
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets.reverse.target')
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties['targets'][0]['reverse'][0]['target'] == {
        'name': 'one',
        '@id': '/testing-custom-embed-targets/one/',
        '@type': ['TestingCustomEmbedTarget', 'Item'],
        'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f',
        'reverse': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'filtered_reverse': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'filtered_reverse1': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'],
        'reverse_uncalculated': ['/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/']
    }
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets.reverse.target', include=['@id'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties == {
        'targets': [
            {'@id': '/testing-custom-embed-targets/one/'}
        ]
    }
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets.reverse.target', include=['@id', 'reverse'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties == {
        'targets': [
            {
                '@id': '/testing-custom-embed-targets/one/',
                'reverse': [
                    {
                        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
                    }
                ]
            }
        ]
    }
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets.reverse.target', include=['@id', 'reverse', 'target'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties == {
        'targets': [
            {
                '@id': '/testing-custom-embed-targets/one/',
                'reverse': [
                    {
                        'target': {
                            '@id': '/testing-custom-embed-targets/one/',
                            'reverse': [
                                '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
                            ]
                        },
                        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
                    }
                ]
            }
        ]
    }


def test_util_path_expand_path_with_frame_slim_embedding(dummy_request, threadlocals, posted_custom_embed_targets_and_sources):
    from snovault.util import Path
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets', include=['reverse'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    p = Path('targets.reverse', include=['target'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    p = Path('targets.reverse.target', include=['name'])
    p.expand_path_with_frame(dummy_request, properties, p.path, p.frame)
    assert properties == {
        'targets': [
            {
                'reverse': [
                    {
                        'target': {
                            'name': 'one'
                        }
                    }
                ]
            }
        ]
    }


def test_util_path_expand_explicit_frame(dummy_request, threadlocals, posted_custom_embed_targets_and_sources):
    from snovault.util import Path
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets', frame='@@embedded')
    p.expand(dummy_request, properties)
    assert properties == {
        'targets': [
            {
                'name': 'one',
                '@id': '/testing-custom-embed-targets/one/',
                '@type': ['TestingCustomEmbedTarget', 'Item'],
                'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f',
                'reverse': [
                    {
                        'name': 'A',
                        'status': 'current',
                        'target': '/testing-custom-embed-targets/one/',
                        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/',
                        '@type': ['TestingCustomEmbedSource', 'Item'],
                        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd'}
                ],
                'filtered_reverse': [
                    {
                        'status': 'current',
                        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd'
                    }
                ],
                'filtered_reverse1': [
                    {
                        'name': 'A',
                        'status': 'current',
                        'target': '/testing-custom-embed-targets/one/',
                        '@id': '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
                    }
                ],
                'reverse_uncalculated': [
                    {
                        'name': 'A',
                        'status': 'current',
                        'target': '/testing-custom-embed-targets/one/'
                    }
                ]
            }
        ]
    }


def test_util_path_expand_exclude(dummy_request, threadlocals, posted_custom_embed_targets_and_sources):
    from snovault.util import Path
    properties = {'targets': ['/testing-custom-embed-targets/one/']}
    p = Path('targets', exclude=['reverse', 'filtered_reverse', 'reverse_uncalculated', '@type'])
    p.expand(dummy_request, properties)
    assert properties == {
        'targets': [
            {
                'name': 'one',
                '@id': '/testing-custom-embed-targets/one/',
                'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f',
                'filtered_reverse1': [
                    '/testing-custom-embed-sources/16157204-8c8f-4672-a1a4-14f4b8021fcd/'
                ]
            }
        ]
    }
