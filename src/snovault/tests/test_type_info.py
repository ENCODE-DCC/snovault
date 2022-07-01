import pytest


def test_type_info_add_types_to_hierarchy():
    from snovault.typeinfo import add_types_to_hierarchy
    hierarchy = {}
    add_types_to_hierarchy(
        [],
        hierarchy,
    )
    assert hierarchy == {}
    add_types_to_hierarchy(
        ['Tissue', 'Biosample', 'Sample', 'Item'],
        hierarchy,
    )
    expected = {
        'Item': {
            'Sample': {
                'Biosample': {
                    'Tissue': {}
                }
            }
        }
    }
    assert expected == hierarchy
    add_types_to_hierarchy(
        ['Primary Cell', 'Biosample', 'Sample', 'Item'],
        hierarchy,
    )
    expected = {
        'Item': {
            'Sample': {
                'Biosample': {
                    'Primary Cell': {},
                    'Tissue': {}
                }
            }
        }
    }
    assert expected == hierarchy
    add_types_to_hierarchy(
        ['HumanDonor', 'Donor', 'Item'],
        hierarchy,
    )
    expected = {
        'Item': {
            'Donor': {
                'HumanDonor': {}
            },
            'Sample': {
                'Biosample': {
                    'Primary Cell': {},
                    'Tissue': {}
                }
            }
        }
    }
    assert expected == hierarchy
