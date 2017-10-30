from snovault import upgrade_step

''' Note these are not relevant to anything beyond testing upgrader '''


@upgrade_step('snowball', '', '2')
@upgrade_step('snowfort', '', '2')
def snowset_0_2(value, system):
    # example upgrade for tests
    if 'status' in value:
        if value['status'] == 'DELETED':
            value['status'] = 'deleted'
        elif value['status'] == 'CURRENT':
            value['status'] = 'submitted'  # there is a dependency on date_released+"released"
