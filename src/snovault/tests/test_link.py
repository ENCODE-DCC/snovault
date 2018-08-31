import pytest


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


def test_links_add(targets, sources, posted_targets_and_sources, session):
    from snovault.storage import Link
    links = sorted([
        (str(link.source_rid), link.rel, str(link.target_rid))
        for link in session.query(Link).all()
    ])
    expected = sorted([
        (sources[0]['uuid'], u'target', targets[0]['uuid']),
        (sources[1]['uuid'], u'target', targets[1]['uuid']),
    ])
    assert links == expected


def test_links_update(targets, sources, posted_targets_and_sources, testapp, session):
    from snovault.storage import Link

    url = '/testing-link-sources/' + sources[1]['uuid']
    new_item = {'name': 'B updated', 'target': targets[0]['name']}
    testapp.put_json(url, new_item, status=200)

    links = sorted([
        (str(link.source_rid), link.rel, str(link.target_rid))
        for link in session.query(Link).all()
    ])
    expected = sorted([
        (sources[0]['uuid'], u'target', targets[0]['uuid']),
        (sources[1]['uuid'], u'target', targets[0]['uuid']),
    ])
    assert links == expected


def test_links_reverse(targets, sources, posted_targets_and_sources, testapp, session):
    target = targets[0]
    res = testapp.get('/testing-link-targets/%s/?frame=object' % target['name'])
    assert res.json['reverse'] == ['/testing-link-sources/%s/' % sources[0]['uuid']]

    # DELETED sources are hidden from the list.
    target = targets[1]
    res = testapp.get('/testing-link-targets/%s/' % target['name'])
    assert res.json['reverse'] == []


def test_links_quoted_ids(posted_targets_and_sources, testapp, session):
    res = testapp.get('/testing-link-targets/quote:name/?frame=object')
    target = res.json
    source = {'name': 'C', 'target': target['@id']}
    testapp.post_json('/testing-link-sources/', source, status=201)
