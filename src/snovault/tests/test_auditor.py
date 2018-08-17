import pytest


def raising_checker(value, system):
    from snovault.auditor import AuditFailure
    if not value.get('checker1'):
        raise AuditFailure('testchecker', 'Missing checker1')


def returning_checker(value, system):
    from snovault.auditor import AuditFailure
    if not value.get('checker1'):
        return AuditFailure('testchecker', 'Missing checker1')


def yielding_checker(value, system):
    from snovault.auditor import AuditFailure
    if not value.get('checker1'):
        yield AuditFailure('testchecker', 'Missing checker1')


def has_condition1(value, system):
    return value.get('condition1')


@pytest.fixture(autouse=True)
def autouse_external_tx(external_tx):
    pass


@pytest.fixture(params=[
    raising_checker,
    returning_checker,
    yielding_checker,
])
def auditor(request):
    from snovault.auditor import Auditor
    auditor = Auditor()
    auditor.add_audit_checker(request.param, 'test')
    return auditor


@pytest.fixture
def auditor_conditions():
    from snovault.auditor import Auditor
    auditor = Auditor()
    auditor.add_audit_checker(raising_checker, 'test', has_condition1)
    return auditor


@pytest.fixture
def audit_dummy_request(registry):
    from pyramid.testing import DummyRequest
    _embed = {}
    request = DummyRequest(registry=registry, _embed=_embed, embed=lambda path: _embed[path])
    return request


def test_audit_pass(auditor, audit_dummy_request):
    value = {'checker1': True}
    audit_dummy_request._embed['/foo/@@object'] = value
    errors = auditor.audit(request=audit_dummy_request, path='/foo/', types='test')
    assert errors == []


def test_audit_failure(auditor, audit_dummy_request):
    value = {}
    audit_dummy_request._embed['/foo/@@object'] = value
    error, = auditor.audit(request=audit_dummy_request, path='/foo/', types='test')
    assert error['detail'] == 'Missing checker1'
    assert error['category'] == 'testchecker'
    assert error['level'] == 0
    assert error['path'] == '/foo/'


def test_audit_conditions(auditor_conditions, audit_dummy_request):
    value = {}
    audit_dummy_request._embed['/foo/@@object'] = value
    assert auditor_conditions.audit(request=audit_dummy_request, path='/foo/', types='test') == []
    value = {'condition1': True}
    audit_dummy_request._embed['/foo/@@object'] = value
    error, = auditor_conditions.audit(request=audit_dummy_request, path='/foo/', types='test')
    assert error['detail'] == 'Missing checker1'
    assert error['category'] == 'testchecker'
    assert error['level'] == 0
    assert error['path'] == '/foo/'


def test_declarative_config(audit_dummy_request):
    from snovault.interfaces import AUDITOR
    from pyramid.config import Configurator
    config = Configurator()
    config.include('snovault.config')
    config.include('snovault.auditor')
    config.include('.testing_auditor')
    config.commit()

    auditor = config.registry[AUDITOR]
    value = {'condition1': True}
    audit_dummy_request._embed['/foo/@@object'] = value
    error, = auditor.audit(request=audit_dummy_request, path='/foo/', types='TestingLinkSourceSno')
    assert error['detail'] == 'Missing checker1'
    assert error['category'] == 'testchecker'
    assert error['level'] == 0
    assert error['path'] == '/foo/'


def test_link_target_audit_fail(testapp):
    target = {'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f', 'status': 'CHECK'}
    testapp.post_json('/testing_link_target_sno', target, status=201)
    res = testapp.get('/%s/@@index-data' % target['uuid']).maybe_follow()
    # make sure linked_uuids are in response. these will be used to run audits
    assert 'linked_uuids' in res.json and len(res.json['linked_uuids']) == 1
    errors_dict = res.json['audit']
    errors_list = []
    for error_type in errors_dict:
        errors_list.extend(errors_dict[error_type])
    errors = [e for e in errors_list if e['name'] == 'testing_link_target_status']
    assert len(errors) == 1
    error, = errors
    assert error['detail'] == 'Missing reverse items'
    assert error['category'] == 'status'
    assert error['level'] == 0
    assert error['path'] == res.json['object']['@id']
    assert res.json['linked_uuids'][0] in error['path']


def test_link_target_audit_pass(testapp):
    target = {'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f', 'status': 'CHECK'}
    testapp.post_json('/testing_link_target_sno', target, status=201)
    source = {'uuid': '16157204-8c8f-4672-a1a4-14f4b8021fcd', 'target': target['uuid']}
    testapp.post_json('/testing_link_source_sno', source, status=201)
    res = testapp.get('/%s/@@index-data' % target['uuid']).maybe_follow()
    # make sure linked_uuids are in response. Should be the target and source
    assert 'linked_uuids' in res.json and len(res.json['linked_uuids']) == 2
    assert source['uuid'] in res.json['linked_uuids']
    assert target['uuid'] in res.json['linked_uuids']
    errors_dict = res.json['audit']
    errors_list = []
    for error_type in errors_dict:
        errors_list.extend(errors_dict[error_type])
    errors = [e for e in errors_list if e['name'] == 'testing_link_target_status']
    assert errors == []


def test_audit_view_using_audit_uuids(testapp, dummy_request, threadlocals):
    # this test uses the other dummy_request fixture
    # get around embed cache by setting as_user=True
    target = {'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f', 'status': 'CHECK'}
    testapp.post_json('/testing_link_target_sno', target, status=201)
    # will not run the audit
    dummy_request._audit_uuids = set()
    res = dummy_request.embed('/testing-link-targets-sno/', target['uuid'], '@@audit', as_user=True)
    assert target['uuid'] in res['@id']
    assert res['audit'] == {}

    # now the audit will fire, since _audit_uuids was set
    dummy_request._audit_uuids = {target['uuid']}
    res2 = dummy_request.embed('/testing-link-targets-sno/', target['uuid'], '@@audit', as_user=True)
    assert target['uuid'] in res2['@id']
    errors_dict = res2['audit']
    errors_list = []
    for error_type in errors_dict:
        errors_list.extend(errors_dict[error_type])
    errors = [e for e in errors_list if e['name'] == 'testing_link_target_status']
    assert len(errors) == 1
    error, = errors
    assert error['detail'] == 'Missing reverse items'
    assert error['category'] == 'status'
    assert error['level'] == 0


def test_audit_view_doesnt_add_to_linked_uuids(testapp, dummy_request, threadlocals):
    import uuid
    # this test uses the other dummy_request fixture
    # get around embed cache by setting as_user=True
    target = {'uuid': '775795d3-4410-4114-836b-8eeecf1d0c2f', 'status': 'CHECK'}
    testapp.post_json('/testing_link_target_sno', target, status=201)

    # test that @@audit does NOT add uuids to _linked_uuids
    # set _indexing_view=True to enable adding to _linked_uuids, which is
    # needed to prove this. Use some random uuid and ensure it stays around
    dummy_request._indexing_view = True
    test_uuid = uuid.uuid4()
    dummy_request._linked_uuids = {test_uuid}
    assert dummy_request._linked_uuids == {test_uuid}
    assert dummy_request._rev_linked_uuids_by_item == {}
    dummy_request._audit_uuids = {target['uuid']}
    res = dummy_request.embed('/testing-link-targets-sno/', target['uuid'], '@@audit', as_user=True)
    assert dummy_request._audit_uuids == {target['uuid']}
    assert dummy_request._linked_uuids == {test_uuid}
    assert dummy_request._rev_linked_uuids_by_item == {}
    assert res['audit']  # ensure audit actually ran
