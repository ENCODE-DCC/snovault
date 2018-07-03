import pytest

targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
    {'name': 'two', 'uuid': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'},
]

sources = [
    {
        'name': 'A',
        'target': '775795d3-4410-4114-836b-8eeecf1d0c2f',
        'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd',
        'status': 'current',
    },
    {
        'name': 'B',
        'target': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377',
        'uuid': '1e152917-c5fd-4aec-b74f-b0533d0cc55c',
        'status': 'deleted',
    },
]


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture
def content(testapp):
    url = '/testing-link-targets-sno/'
    for item in targets:
        testapp.post_json(url, item, status=201)

    url = '/testing-link-sources-sno/'
    for item in sources:
        testapp.post_json(url, item, status=201)

def test_referenced_uuids_unset(content, dummy_request, threadlocals):
    # without setting _indexing_view =True on the request,
    # _referenced_uuids are not tracked
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._referenced_uuids == set()


def test_referenced_uuids_object(content, dummy_request, threadlocals):
    # needed to track _referenced_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@object')
    assert dummy_request._referenced_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd'}


def test_referenced_uuids_embedded(content, dummy_request, threadlocals):
    # needed to track _referenced_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@embedded')
    assert dummy_request._referenced_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}


def test_referenced_uuids_page(content, dummy_request, threadlocals):
    # needed to track _referenced_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@page')
    assert dummy_request._referenced_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}


def test_referenced_uuids_expand_target(content, dummy_request, threadlocals):
    # needed to track _referenced_uuids
    dummy_request._indexing_view = True
    dummy_request.embed('/testing-link-sources-sno/', sources[0]['uuid'], '@@expand?expand=target')
    # expanding does not add to the embedded_list
    assert dummy_request._referenced_uuids == {'16157204-8c8f-4672-a1a4-14f4b8021fcd', '775795d3-4410-4114-836b-8eeecf1d0c2f'}
