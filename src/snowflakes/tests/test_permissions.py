import pytest


def _remote_user_testapp(app, remote_user):
    from webtest import TestApp
    environ = {
        'HTTP_ACCEPT': 'application/json',
        'REMOTE_USER': str(remote_user),
    }
    return TestApp(app, environ)


@pytest.fixture
def remote_user_testapp(app, remote_user):
    return _remote_user_testapp(app, remote_user)


@pytest.fixture
def disabled_user(testapp, lab, award):
    item = {
        'first_name': 'ENCODE',
        'last_name': 'Submitter',
        'email': 'no_login_submitter@example.org',
        'submits_for': [lab['@id']],
        'status': 'disabled',
    }
    # User @@object view has keys omitted.
    res = testapp.post_json('/user', item)
    return testapp.get(res.location).json


@pytest.fixture
def other_lab(testapp):
    item = {
        'title': 'Other lab',
        'name': 'other-lab',
    }
    return testapp.post_json('/lab', item, status=201).json['@graph'][0]


@pytest.fixture
def wrangler_testapp(wrangler, app, external_tx, zsa_savepoints):
    return _remote_user_testapp(app, wrangler['uuid'])


@pytest.fixture
def submitter_testapp(submitter, app, external_tx, zsa_savepoints):
    return _remote_user_testapp(app, submitter['uuid'])


@pytest.fixture
def viewing_group_member_testapp(viewing_group_member, app, external_tx, zsa_savepoints):
    return _remote_user_testapp(app, viewing_group_member['uuid'])


@pytest.fixture
def remc_member_testapp(remc_member, app, external_tx, zsa_savepoints):
    return _remote_user_testapp(app, remc_member['uuid'])


def test_user_view_details_admin(submitter, access_key, testapp):
    res = testapp.get(submitter['@id'])
    assert 'email' in res.json
    assert 'access_keys' in res.json
    assert 'access_key_id' in res.json['access_keys'][0]


def test_users_view_details_self(submitter, access_key, submitter_testapp):
    res = submitter_testapp.get(submitter['@id'])
    assert 'email' in res.json
    assert 'access_keys' in res.json
    assert 'access_key_id' in res.json['access_keys'][0]


def test_users_patch_self(submitter, access_key, submitter_testapp):
    submitter_testapp.patch_json(submitter['@id'], {})


def test_users_post_disallowed(submitter, access_key, submitter_testapp):
    item = {
        'first_name': 'ENCODE',
        'last_name': 'Submitter2',
        'email': 'encode_submitter2@example.org',
    }
    submitter_testapp.post_json('/user', item, status=403)


def test_users_view_basic_authenticated(submitter, authenticated_testapp):
    res = authenticated_testapp.get(submitter['@id'])
    assert 'title' in res.json
    assert 'email' not in res.json
    assert 'access_keys' not in res.json


def test_users_view_basic_anon(submitter, anontestapp):
    res = anontestapp.get(submitter['@id'])
    assert 'title' in res.json
    assert 'email' not in res.json
    assert 'access_keys' not in res.json


def test_users_view_basic_indexer(submitter, indexer_testapp):
    res = indexer_testapp.get(submitter['@id'])
    assert 'title' in res.json
    assert 'email' not in res.json
    assert 'access_keys' not in res.json


def test_submitter_patch_lab_disallowed(submitter, other_lab, submitter_testapp):
    res = submitter_testapp.get(submitter['@id'])
    lab = {'lab': other_lab['@id']}
    submitter_testapp.patch_json(res.json['@id'], lab, status=422)  # is that the right status?


def test_wrangler_patch_lab_allowed(submitter, other_lab, wrangler_testapp):
    res = wrangler_testapp.get(submitter['@id'])
    lab = {'lab': other_lab['@id']}
    wrangler_testapp.patch_json(res.json['@id'], lab, status=200)


def test_submitter_patch_submits_for_disallowed(submitter, other_lab, submitter_testapp):
    res = submitter_testapp.get(submitter['@id'])
    submits_for = {'submits_for': res.json['submits_for'] + [other_lab['@id']]}
    submitter_testapp.patch_json(res.json['@id'], submits_for, status=422)


def test_wrangler_patch_submits_for_allowed(submitter, other_lab, wrangler_testapp):
    res = wrangler_testapp.get(submitter['@id'])
    submits_for = {'submits_for': res.json['submits_for'] + [other_lab['@id']]}
    wrangler_testapp.patch_json(res.json['@id'], submits_for, status=200)


def test_submitter_patch_groups_disallowed(submitter, other_lab, submitter_testapp):
    res = submitter_testapp.get(submitter['@id'])
    groups = {'groups': res.json['groups'] + ['admin']}
    submitter_testapp.patch_json(res.json['@id'], groups, status=422)


def test_wrangler_patch_groups_allowed(submitter, other_lab, wrangler_testapp):
    res = wrangler_testapp.get(submitter['@id'])
    groups = {'groups': res.json['groups'] + ['admin']}
    wrangler_testapp.patch_json(res.json['@id'], groups, status=200)


def test_submitter_patch_viewing_groups_disallowed(submitter, other_lab, submitter_testapp):
    res = submitter_testapp.get(submitter['@id'])
    vgroups = {'viewing_groups': res.json['viewing_groups'] + ['GGR']}
    submitter_testapp.patch_json(res.json['@id'], vgroups, status=422)


def test_wrangler_patch_viewing_groups_disallowed(submitter, other_lab, wrangler_testapp):
    res = wrangler_testapp.get(submitter['@id'])
    vgroups = {'viewing_groups': res.json['viewing_groups'] + ['GGR']}
    wrangler_testapp.patch_json(res.json['@id'], vgroups, status=200)


def test_disabled_user_denied_authenticated(authenticated_testapp, disabled_user):
    authenticated_testapp.get(disabled_user['@id'], status=403)


def test_disabled_user_denied_submitter(submitter_testapp, disabled_user):
    submitter_testapp.get(disabled_user['@id'], status=403)


def test_disabled_user_wrangler(wrangler_testapp, disabled_user):
    wrangler_testapp.get(disabled_user['@id'], status=200)


def test_labs_view_wrangler(wrangler_testapp, other_lab):
    labs = wrangler_testapp.get('/labs/', status=200)
    assert(len(labs.json['@graph']) == 1)


def test_snowflake_accession_patch_admin(testapp, snowflake):
    new_accession = 'SNOFL123ABC'
    testapp.patch_json(snowflake['@id'], {'accession': new_accession}, status=200)


def test_snowflake_accession_patch_wrangler(wrangler_testapp, snowflake):
    new_accession = 'SNOFL123ABC'
    wrangler_testapp.patch_json(snowflake['@id'], {'accession': new_accession}, status=200)


def test_snowflake_accession_patch_submitter(submitter_testapp, snowflake):
    new_accession = 'SNOFL123ABC'
    # permission 'import_items' required
    submitter_testapp.patch_json(snowflake['@id'], {'accession': new_accession}, status=422)


def test_snowflake_accession_put_admin(testapp, snowflake):
    snowflake_id = snowflake['@id']
    old_accession = snowflake['accession']
    # Can't resubmit @type/@id.
    snowflake.pop('@id', None)
    snowflake.pop('@type', None)
    testapp.put_json(snowflake_id, snowflake, status=200)
    # Change allowable field.
    snowflake['status'] = 'released'
    testapp.put_json(snowflake_id, snowflake, status=200)
    # Change nonallowable field.
    assert 'accession' in snowflake
    snowflake['accession'] = 'SNOFL123ABC'
    res = testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Submit with blank accession.
    snowflake.pop('accession', None)
    assert 'accession' not in snowflake
    res = testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Change uuid.
    snowflake['accession'] = old_accession
    from uuid import uuid4
    snowflake['uuid'] = str(uuid4())
    res = testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'uuid may not be changed'


def test_snowflake_accession_put_submitter(submitter_testapp, snowflake):
    snowflake_id = snowflake['@id']
    old_accession = snowflake['accession']
    # Can't resubmit @type/@id.
    snowflake.pop('@id', None)
    snowflake.pop('@type', None)
    submitter_testapp.put_json(snowflake_id, snowflake, status=200)
    # Change allowable field.
    snowflake['status'] = 'released'
    submitter_testapp.put_json(snowflake_id, snowflake, status=200)
    # Submitters can't edit released objects.
    snowflake['status'] = 'in progress'
    submitter_testapp.put_json(snowflake_id, snowflake, status=403)
    # Change nonallowable field.
    assert 'accession' in snowflake
    snowflake['accession'] = 'SNOFL123ABC'
    res = submitter_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Submit with blank accession.
    snowflake.pop('accession', None)
    assert 'accession' not in snowflake
    res = submitter_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Change uuid.
    snowflake['accession'] = old_accession
    from uuid import uuid4
    snowflake['uuid'] = str(uuid4())
    res = submitter_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'uuid may not be changed'


def test_snowflake_accession_put_wrangler(wrangler_testapp, snowflake):
    snowflake_id = snowflake['@id']
    old_accession = snowflake['accession']
    # Can't resubmit @type/@id.
    snowflake.pop('@id', None)
    snowflake.pop('@type', None)
    wrangler_testapp.put_json(snowflake_id, snowflake, status=200)
    # Change allowable field.
    snowflake['status'] = 'released'
    wrangler_testapp.put_json(snowflake_id, snowflake, status=200)
    # Wranglers can edit released objects.
    snowflake['status'] = 'in progress'
    wrangler_testapp.put_json(snowflake_id, snowflake, status=200)
    # Change nonallowable field.
    assert 'accession' in snowflake
    snowflake['accession'] = 'SNOFL123ABC'
    res = wrangler_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Submit with blank accession.
    snowflake.pop('accession', None)
    assert 'accession' not in snowflake
    res = wrangler_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'must specify original accession'
    # Change uuid.
    snowflake['accession'] = old_accession
    from uuid import uuid4
    snowflake['uuid'] = str(uuid4())
    res = wrangler_testapp.put_json(snowflake_id, snowflake, status=422)
    assert res.json['errors'][0]['description'] == 'uuid may not be changed'
