def test_batch_upgrade_1(testapp, invalid_snowball):
    '''
    patch = {
        'schema_version': '1',
        'status': 'CURRENT',
    }
    res = testapp.patch_json(invalid_snowball['@id'] + '?validate=false', patch)
    '''

    assert invalid_snowball['status'] == 'CURRENT'
    assert invalid_snowball['schema_version'] == '1'
    upgrade = testapp.post_json(
        '/batch_upgrade', {'batch': [invalid_snowball['uuid']]}, status=200)

    res = testapp.get(invalid_snowball['@id'])
    assert res.json['schema_version'] == '2'
    assert res.json['status'] == 'submitted'

    assert upgrade.json['results'][0] == [
        'snowball',
        invalid_snowball['uuid'],
        False,  # updated
        False,   # errors
    ]


def test_batch_upgrade_2(testapp, snowball):
    patch = {
        'schema_version': '1',
        'status': 'FLYBARGS',
    }
    res = testapp.patch_json(snowball['@id'] + '?validate=false', patch)

    assert snowball['status'] == 'FLYBARGS'
    assert snowball['schema_version'] == '1'
    upgrade = testapp.post_json(
        '/batch_upgrade', {'batch': [snowball['uuid']]}, status=200)

    res = testapp.get(snowball['@id'])
    assert res.json['schema_version'] == '2'
    assert res.json['status'] == 'submitted'

    assert upgrade.json['results'][0] == [
        'snowball',
        snowball['uuid'],
        False,  # updated
        False,   # errors
    ]
