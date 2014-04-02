from seamus.collectors import iscsiadm

__author__ = 'achmed'

import seamus.agent

plugin = seamus.agent.plugin('hvnode')
lun = plugin.make_type('lun', luid='lun', target='target', initiator='initiator')()
target = plugin.make_type('target', target_iqn='target_iqn', portal='current_portal')()

idata = iscsiadm.IScsiAdmDataInterface.iscsiadm_session_properties()

for target_iqn,v in idata.iteritems():
    for l in v['luns'].itervalues():
        l['target'] = target_iqn
        l['initiator'] = v['iface_initiatorname']
        lun.update(l)

    del v['luns']

    v['target_iqn'] = target_iqn

    target.update(v)

import pprint
pprint.pprint(lun.data)
pprint.pprint(target.data)
