def test_audit_item_schema_validation(testapp, snowball):
    testapp.patch_json(snowball['@id'] + '?validate=false', {'disallowed': 'errs'})
    res = testapp.get(snowball['@id'] + '@@index-data')
    errors = res.json['audit']
    errors_list = []
    for error_type in errors:
        errors_list.extend(errors[error_type])
    assert any(
        error['category'] == 'validation error' and error['name'] == 'audit_item_schema'
        for error in errors_list)


def test_audit_item_schema_upgrade_failure(testapp, snowball):
    testapp.patch_json(snowball['@id'] + '?validate=false', {'schema_version': '999'})
    res = testapp.get(snowball['@id'] + '@@index-data')
    errors = res.json['audit']
    errors_list = []
    for error_type in errors:
        errors_list.extend(errors[error_type])
    assert any(
        error['category'] == 'upgrade failure' and error['name'] == 'audit_item_schema'
        for error in errors_list)


def test_audit_item_schema_upgrade_ok(testapp, snowball):
    patch = {
        'schema_version': '1',
        'status': 'CURRENT',
    }
    testapp.patch_json(snowball['@id'] + '?validate=false', patch)
    res = testapp.get(snowball['@id'] + '@@index-data')
    # errors = [e for e in res.json['audit'] if e['name'] == 'audit_item_schema']
    # assert not errors
    errors = res.json['audit']
    errors_list = []
    for error_type in errors:
        errors_list.extend(errors[error_type])
    assert not any(error['name'] == 'audit_item_schema' for error in errors_list)


def test_audit_item_schema_upgrade_validation_failure(testapp, snowball):
    patch = {
        'schema_version': '1',
        'status': 'UNKNOWN',
    }
    testapp.patch_json(snowball['@id'] + '?validate=false', patch)
    res = testapp.get(snowball['@id'] + '@@index-data')
    errors = res.json['audit']
    errors_list = []
    for error_type in errors:
        errors_list.extend(errors[error_type])
    assert any(
        error['category'] == 'validation error: status' and error['name'] == 'audit_item_schema'
        for error in errors_list)


def test_audit_item_schema_permission(testapp, snowflake, embed_testapp):
    patch = {
        'type': 'wet',
        'status': 'deleted',
    }
    testapp.patch_json(snowflake['@id'], patch)
    res = embed_testapp.get(snowflake['@id'] + '/@@audit-self')
    errors_list = res.json['audit']
    assert not any(error['name'] == 'audit_item_schema' for error in errors_list)
