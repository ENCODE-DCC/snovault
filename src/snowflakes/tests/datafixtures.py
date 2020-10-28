import pytest


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture
def lab(testapp):
    item = {
        'name': 'encode-lab',
        'title': 'ENCODE lab',
    }
    return testapp.post_json('/lab', item).json['@graph'][0]


@pytest.fixture
def remc_lab(testapp):
    item = {
        'name': 'remc-lab',
        'title': 'REMC lab',
    }
    return testapp.post_json('/lab', item).json['@graph'][0]


@pytest.fixture
def admin(testapp):
    item = {
        'first_name': 'Test',
        'last_name': 'Admin',
        'email': 'admin@example.org',
        'groups': ['admin'],
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def wrangler(testapp):
    item = {
        # some objects have linkEnums
        'uuid': '4c23ec32-c7c8-4ac0-affb-04befcc881d4',
        'first_name': 'Wrangler',
        'last_name': 'Admin',
        'email': 'wrangler@example.org',
        'groups': ['admin'],
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def submitter(testapp, lab, award):
    item = {
        'first_name': 'ENCODE',
        'last_name': 'Submitter',
        'email': 'encode_submitter@example.org',
        'submits_for': [lab['@id']],
        'viewing_groups': [award['viewing_group']],
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def access_key(testapp, submitter):
    description = 'My programmatic key'
    item = {
        'user': submitter['@id'],
        'description': description,
    }
    res = testapp.post_json('/access_key', item)
    result = res.json['@graph'][0].copy()
    result['secret_access_key'] = res.json['secret_access_key']
    return result


@pytest.fixture
def viewing_group_member(testapp, award):
    item = {
        'first_name': 'Viewing',
        'last_name': 'Group',
        'email': 'viewing_group_member@example.org',
        'viewing_groups': [award['viewing_group']],
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def remc_member(testapp, remc_lab):
    item = {
        'first_name': 'REMC',
        'last_name': 'Member',
        'email': 'remc_member@example.org',
        'submits_for': [remc_lab['@id']],
        'viewing_groups': ['REMC'],
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def award(testapp):
    item = {
        'name': 'encode3-award',
        'rfa': 'ENCODE3',
        'project': 'ENCODE',
        'viewing_group': 'ENCODE',
    }
    return testapp.post_json('/award', item).json['@graph'][0]


@pytest.fixture
def invalid_award(testapp):
    item = {
        'name': 'award names cannot have spaces',
        'rfa': 'ENCODE3',
        'project': 'ENCODE',
        'viewing_group': 'ENCODE',
    }
    return testapp.post_json('/award' + '?validate=false', item).json['@graph'][0]


RED_DOT = """data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA
AAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO
9TXL0Y4OHwAAAABJRU5ErkJggg=="""


@pytest.fixture
def attachment():
    return {'download': 'red-dot.png', 'href': RED_DOT}


@pytest.fixture
def snowball(testapp, lab, award):
    item = {
        'award': award['@id'],
        'lab': lab['@id'],
        'method': 'hand-packed',
    }
    return testapp.post_json('/snowball', item).json['@graph'][0]


@pytest.fixture
def invalid_snowball(testapp, lab, award):
    item = {
        'award': award['uuid'],
        'lab': lab['uuid'],
        'method': 'hand-packed',
        'schema_version': '1',
        'status': 'FLYBARGS',
    }
    return testapp.post_json('/snowball/' + '?validate=false', item).json['@graph'][0]


@pytest.fixture
def invalid_snowball2(testapp, lab, award):
    item = {
        'award': award['uuid'],
        'lab': lab['uuid'],
        'method': 'hand-packed',
        'schema_version': '1',
        'status': 'FLINGERNEERS',
        'alternate_accessions': ['THE THING THAT SHOULD NOT BE']
    }
    return testapp.post_json('/snowball/' + '?validate=false', item).json['@graph'][0]


@pytest.fixture
def snowflake(testapp, lab, award, snowball):
    item = {
        'snowset': snowball['@id'],
        'type': 'fluffy',
        'status': 'in progress',
        'award': award['@id'],
        'lab': lab['@id'],
    }
    return testapp.post_json('/snowflake', item).json['@graph'][0]


@pytest.fixture
def targets():
    targets = [
        {'name': 'one', 'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f'},
        {'name': 'two', 'uuid': 'd6784f5e-48a1-4b40-9b11-c8aefb6e1377'},
        {'name': 'quote:name', 'uuid': '0e627b3b-f5d2-41db-ac34-8e97bb8a028c'},
    ]
    return targets


@pytest.fixture
def sources():
    sources = [
        {
            'name': 'A',
            'target': 'one',
            'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd',
            'status': 'current',
        },
        {
            'name': 'B',
            'target': 'two',
            'uuid': '1e152917-c5fd-4aec-b74f-b0533d0cc55c',
            'status': 'deleted',
        },
    ]
    return sources


@pytest.fixture
def posted_targets_and_sources(testapp, targets, sources):
    url = '/testing-link-targets/'
    for item in targets:
        testapp.post_json(url, item, status=201)

    url = '/testing-link-sources/'
    for item in sources:
        testapp.post_json(url, item, status=201)


@pytest.fixture
def posted_custom_embed_targets_and_sources(testapp, targets, sources):
    url = '/testing-custom-embed-targets/'
    for item in targets:
        testapp.post_json(url, item, status=201)

    url = '/testing-custom-embed-sources/'
    for item in sources:
        testapp.post_json(url, item, status=201)
