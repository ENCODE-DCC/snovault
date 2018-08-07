from snovault.schema_utils import validate
import pytest


targets = [
    {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
]


@pytest.fixture
def content(testapp):
    url = '/testing-link-targets/'
    for item in targets:
        testapp.post_json(url, item, status=201)


def test_uniqueItems_validates_normalized_links(content, threadlocals):
    schema = {
        'uniqueItems': True,
        'items': {
            'linkTo': 'TestingLinkTarget',
        }
    }
    uuid = targets[0]['uuid']
    data = [
        uuid,
        '/testing-link-targets/{}'.format(uuid),
    ]
    validated, errors = validate(schema, data)
    assert len(errors) == 1
    assert (
        errors[0].message == "['{}', '{}'] has non-unique elements".format(
            uuid, uuid)
    )
