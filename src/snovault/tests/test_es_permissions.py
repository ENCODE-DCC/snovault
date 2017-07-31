import pytest
from snovault.elasticsearch.cached_views import filter_embedded
from copy import deepcopy


def test_filtered_embedded_admin(embedded_lab, effective_princ_admin):
    original = deepcopy(embedded_lab)
    filtered = filter_embedded(embedded_lab, effective_princ_admin)
    assert filtered == embedded_lab
    assert original == embedded_lab


def test_filtered_embedded_no_perms(embedded_lab):
    original = deepcopy(embedded_lab)
    filter_embedded(embedded_lab, [])
    assert original != embedded_lab
    assert embedded_lab == {'lab': {'error': "no view permissions"}}


def test_filtered_embedded_second_level_no_perms(embedded_2nd, effective_princ_not_logged_in):
    original = deepcopy(embedded_2nd)
    filter_embedded(embedded_2nd, effective_princ_not_logged_in)
    assert original != embedded_2nd
    assert embedded_2nd['lab']['award'] == {'error': "no view permissions"}


def test_filtered_embedded_array(embedded_3rd_array, effective_princ_lab):
    modified = deepcopy(embedded_3rd_array)
    original = deepcopy(modified)

    filter_embedded(modified, effective_princ_lab)
    assert modified != original
    # no perms for this one
    assert modified['biosample']['biosource'][1] == {'error': "no view permissions"}
    # but we can see this one
    assert 'uuid' in modified['biosample']['biosource'][0].keys()
    assert 'uuid' in modified['biosample']['biosource'][0]['individual']['organism'].keys()


@pytest.fixture()
def effective_princ_not_logged_in():
    return ['system.Everyone']


@pytest.fixture()
def effective_princ_lab():
    return ['lab.labtest', 'system.Everyone']


@pytest.fixture()
def embedded_2nd():
    return {'lab': {'fax': '000-000-0000', 'city': 'Boston', 'name': 'test-4dn-lab',
                    'state': 'MA', '@type': ['Lab', 'Item'],
                    'uuid': '828cd4fe-ebb0-4b22-a94a-d2e3a36cc988',
                    'display_title': '4DN Testing Lab', 'link_id': '~labs~test-4dn-lab~',
                    'principals_allowed': {'view': ['system.Everyone']},
                    'award': {'name': 'test_award',
                              'principals_allowed': {'view': ['group.admin']}
                              }
                    }
            }


@pytest.fixture()
def embedded_3rd_array():
    return {'biosample': {
        'status': 'in review by lab',
        'accession': '4DNBS2548734',
        'biosource': [
            {
                'status': 'in review by lab',
                'accession': '4DNSR9R56R7W',
                'individual': {
                    'organism': {
                        '@id': '/organisms/human/',
                        '@type': [
                            'Organism',
                            'Item'
                        ],
                        'uuid': '7745b647-ff15-4ff3-9ced-b897d4e2983c',
                        'principals_allowed': {
                            'view': [
                                'system.Everyone'
                            ],
                        }
                    }
                },
                'schema_version': '2',
                '@id': '/biosources/4DNSR9R56R7W/',
                '@type': [
                    'Biosource',
                    'Item'
                ],
                'uuid': '331111bc-8535-4448-903e-854af460b254',
                'principals_allowed': {
                    'view': [
                        'group.admin',
                        'lab.labtest',
                    ],
                },
'biosource_name': 'GM12878'
},

            {
                'uuid': '331111bc-8535-4448-903e-854af460b254',
                'principals_allowed': {
                    'view': [
                        'group.admin',
                    ],
                    },
                'biosource_name': 'dumb, biosource'
}
],
'treatments': [
    {
        'url': 'https://www.cyclingnews.com',
        'status': 'in review by lab',
        'aliases': [
            'Awesome:Treatment'
        ],
        'rnai_type': 'siRNA',
        'description': 'RNAi treatment for rS3',
        'date_created': '2017-07-31T14:31:24.815183+00:00',
        'schema_version': '1',
        'target_sequence': 'ATGCATGC',
        '@id': '/treatments-rnai/686b362f-4eb6-4a9c-8173-3ab267307e3b/',
        '@type': [
            'TreatmentRnai',
            'Treatment',
            'Item'
        ],
        'uuid': '686b362f-4eb6-4a9c-8173-3ab267307e3b',
        'display_title': 'TreatmentRnai from 2017-07-31',
        'link_id': '~treatments-rnai~686b362f-4eb6-4a9c-8173-3ab267307e3b~',
        'principals_allowed': {
            'view': [
                'award.b0b9c607-f8b4-4f02-93f4-9895b461334b',
                'group.admin',
                'group.read-only-admin',
                'lab.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989',
                'remoteuser.EMBED',
                'remoteuser.INDEXER'
            ],
            'edit': [
                'group.admin',
                'submits_for.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989'
            ],
            'audit': [
                'system.Everyone'
            ]
        },
        'treatment_type': 'siRNA for Gene:PARK2, FMR1'
    },
    {
        'url': 'https://www.cyclingnews.com',
        'status': 'in review by lab',
        'aliases': [
            'Awesome:Treatment2'
        ],
        'rnai_type': 'shRNA',
        'description': 'RNAi treatment for rS5',
        'date_created': '2017-07-31T14:31:24.873541+00:00',
        'schema_version': '1',
        'target_sequence': 'ATGCATGC',
        '@id': '/treatments-rnai/686b362f-4eb6-4a9c-8173-3ab267307a8d/',
        '@type': [
            'TreatmentRnai',
            'Treatment',
            'Item'
        ],
        'uuid': '686b362f-4eb6-4a9c-8173-3ab267307a8d',
        'display_title': 'TreatmentRnai from 2017-07-31',
        'link_id': '~treatments-rnai~686b362f-4eb6-4a9c-8173-3ab267307a8d~',
        'principals_allowed': {
            'view': [
                'award.b0b9c607-f8b4-4f02-93f4-9895b461334b',
                'group.admin',
                'group.read-only-admin',
                'lab.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989',
                'remoteuser.EMBED',
                'remoteuser.INDEXER'
            ],
            'edit': [
                'group.admin',
                'submits_for.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989'
            ],
            'audit': [
                'system.Everyone'
            ]
        },
        'treatment_type': 'shRNA for Gene:PARK2, FMR1'
    }
    ],
        'description': 'GM12878 prepared for Hi-C, batch 2',
        'date_created': '2017-07-31T14:31:25.014388+00:00',
        'modifications': [
    {
        'url': 'https://www.cyclingnews.com',
        'status': 'in review by lab',
        'description': 'Stable Tranfection of GFP construct',
        'date_created': '2017-07-31T14:31:23.390224+00:00',
        'schema_version': '1',
        'modification_type': 'Stable Transfection',
        '@id': '/modifications/431106bc-8535-4448-903e-854af460b265/',
        '@type': [
            'Modification',
            'Item'
        ],
        'uuid': '431106bc-8535-4448-903e-854af460b265',
        'display_title': 'Stable Transfection for Gene:PARK2, FMR1',
        'link_id': '~modifications~431106bc-8535-4448-903e-854af460b265~',
        'principals_allowed': {
            'view': [
                'award.b0b9c607-f8b4-4f02-93f4-9895b461334b',
                'group.admin',
                'group.read-only-admin',
                'lab.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989',
                'remoteuser.EMBED',
                'remoteuser.INDEXER'
            ],
            'edit': [
                'group.admin',
                'submits_for.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989'
            ],
            'audit': [
                'system.Everyone'
            ]
        },
        'modification_name': 'Stable Transfection for Gene:PARK2, FMR1',
        'modification_name_short': 'Stable Transfection for Gene:PARK2, FMR1'
    },
    {
        'url': 'https://www.cyclingnews.com',
        'status': 'in review by lab',
        'description': 'Other type of modification',
        'date_created': '2017-07-31T14:31:23.419011+00:00',
        'schema_version': '1',
        'modification_type': 'Other',
        '@id': '/modifications/431106bc-8535-4448-903e-854af460b276/',
        '@type': [
            'Modification',
            'Item'
        ],
        'uuid': '431106bc-8535-4448-903e-854af460b276',
        'display_title': 'Other',
        'link_id': '~modifications~431106bc-8535-4448-903e-854af460b276~',
        'principals_allowed': {
            'view': [
                'group.admin',
                'group.read-only-admin',
                'lab.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989',
                'remoteuser.EMBED',
                'remoteuser.INDEXER'
            ],
            'edit': [
                'group.admin',
                'submits_for.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989'
            ],
            'audit': [
                'system.Everyone'
            ]
        },
        'modification_name': 'Other',
        'modification_name_short': 'Other'
    }
],
'schema_version': '1',
'@id': '/biosamples/4DNBS2548734/',
'@type': [
    'Biosample',
    'Item'
],
'uuid': '231111bc-8535-4448-903e-854af460b242',
'display_title': '4DNBS2548734',
'link_id': '~biosamples~4DNBS2548734~',
'principals_allowed': {
    'view': [
        'group.admin',
        'lab.labtest',
    ],
},
'biosource_summary': 'GM12878'
}
}


@pytest.fixture()
def embedded_lab():
    return {'lab': {'fax': '000-000-0000', 'city': 'Boston', 'name': 'test-4dn-lab', 'state': 'MA', 'title': '4DN Testing Lab', 'status': 'current', 'country': 'USA', 'address1': 'Biomedical Bioinfomatics', 'address2': '10 Schattuck Street', 'postal_code': '02115', 'date_created': '2017-07-31T14:31:13.532200+00:00', 'institute_name': 'Harvard Medical School', 'schema_version': '1', 'institute_label': 'HMS', '@id': '/labs/test-4dn-lab/', '@type': ['Lab', 'Item'], 'uuid': '828cd4fe-ebb0-4b22-a94a-d2e3a36cc988', 'display_title': '4DN Testing Lab', 'link_id': '~labs~test-4dn-lab~', 'principals_allowed': {'view': ['system.Everyone'], 'edit': ['group.admin', 'submits_for.828cd4fe-ebb0-4b22-a94a-d2e3a36cc988'], 'audit': ['system.Everyone']}}}


@pytest.fixture()
def effective_princ_admin():
    return ['auth0.4dndcic@gmail.com', 'system.Everyone', 'system.Authenticated', 'submits_for.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989', 'lab.828cd4fe-ebb0-4b36-a94a-d2e3a36cc989', 'userid.986b362f-4eb6-4a9c-8173-3ab267307e3a', 'group.submitter', 'award.b0b9c607-f8b4-4f02-93f4-9895b461334b', 'group.admin', 'remoteuser.EMBED', 'viewing_group.4DN']
