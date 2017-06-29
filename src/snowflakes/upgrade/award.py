from snovault import upgrade_step

''' Note these are not relevant to anything beyond testing upgrader '''


@upgrade_step('award', '', '2')
def award_0_2(value, system):
    # Sample upgrades with tests

    rfa_mapping = ['ENCODE2', 'ENCODE2-Mouse']
    if value['rfa'] in rfa_mapping:
        value['status'] = 'disabled'
    else:
        value['status'] = 'current'

    if 'url' in value:
        if value['url'] == '':
            del value['url']


@upgrade_step('award', '2', '3')
def award_2_3(value, system):

    if value['viewing_group'] == 'ENCODE3':
        value['viewing_group'] = 'ENCODE'
