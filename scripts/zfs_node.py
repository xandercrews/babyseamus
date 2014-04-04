__author__ = 'achmed'

import seamus.agent
from seamus.collectors import zfs, stmf, itadm

itdata = itadm.ITAdmDataInterface.itadm_target_properties()
stdata = stmf.STMFDataInterface.stmf_list_targets()
zpoolstatusdata = zfs.ZFSDataInterface.zpool_status()
zpoolpropdata = zfs.ZFSDataInterface.zpool_properties()
zfsvoldata = zfs.ZFSDataInterface.zfs_volume_properties()

import pprint

# print '******itdata*********'
# pprint.pprint(itdata)
# print '******stdata*********'
# pprint.pprint(stdata)
# print '******zpoolstatusdata*********'
# pprint.pprint(zpoolstatusdata)
# print '******zpoolpropdata*********'
# pprint.pprint(zpoolpropdata)
# print '******zfsvoldata*********'
# pprint.pprint(zfsvoldata)

plugin = seamus.agent.plugin('zfsnode')

lun = plugin.make_type('lun', luid='luid')()
hostgroup = plugin.make_type('hostgroup', name='name')()
targetportalgroup = plugin.make_type('targetportalgroup', name='name')()
target = plugin.make_type('target', target_iqn='target_iqn')()
# targetgroup = plugin.make_type('targetgroup')()
view = plugin.make_type('view', luid='luid', viewnum='viewnum')()
zpool = plugin.make_type('zpool', name='name')()
zvol = plugin.make_type('zvol', name='name')()
disk = plugin.make_type('disk', devicename='devicename')()
vdev = plugin.make_type('vdev', zpoolname='zpoolname', devicename='devicename')()
cachedev = plugin.make_type('cachedev', devicename='devicename', zpoolname='zpoolname')()
sparedev = plugin.make_type('sparedev', devicename='devicename', zpoolname='zpoolname')()
zildev = plugin.make_type('zildev', devicename='devicename', zpoolname='zpoolname')()
zfssnapshot = plugin.make_type('zfssnapshot', name='name')()

# targets
for target_iqn, t in itdata['targets'].iteritems():
    t['target_iqn'] = target_iqn
    t['targetportalgroup'] = map(str.strip, t['tpg-tags'].split('='))[0]
    del t['tpg-tags']
    target.update(t)

# tpgs
for tpg, t in itdata['tpgs'].iteritems():
    t['name'] = tpg
    targetportalgroup.update(t)

# hgs
for hg, g in stdata['hgs'].iteritems():
    g['name'] = hg
    hostgroup.update(g)

# luns and their views
for luid, l in stdata['luns'].iteritems():
    if 'views' in l:
        for viewnum, v in l['views'].items():
            v['viewnum'] = viewnum
            v['luid'] = luid
            view.update(v)
        del l['views']
    l['luid'] = luid
    lun.update(l)

# zpools and their disks
for zpoolname, z in zpoolstatusdata.iteritems():
    z['name'] = zpoolname

    # pool devices
    if 'vdev' in z:
        for devicename, v in z['vdev'].iteritems():
            v['devicename'] = devicename
            v['zpoolname'] = zpoolname

            if 'vdev' in v:
                # there are nested vdevs- record 1st level devices as vdevs
                for devicename2, v2 in v['vdev'].iteritems():
                    v2['parentdev'] = devicename
                    v2['devicename'] = devicename2
                    v2['zpoolname'] = zpoolname
                    disk.update(v2)

                del v['vdev']

                vdev.update(v)
            else:
                # there are not nested vdevs, record 1st level devices as disks
                disk.update(v)
        del z['vdev']

    # process cache devices
    if 'cache' in z:
        for devicename, c in z['cache'].iteritems():
            c['devicename'] = devicename
            c['zpoolname'] = zpoolname
            cachedev.update(c)

        del z['cache']

    # spare devices
    if 'spares' in z:
        for devicename, s in z['spares'].iteritems():
            s['devicename'] = devicename
            s['zpoolname'] = zpoolname
            sparedev.update(s)

        del z['spares']

    # log devices
    if 'log' in z:
        for devicename, l in z['log'].iteritems():
            l['devicename'] = devicename
            l['zpoolname'] = zpoolname
            zildev.update(l)

        del z['log']

    zpool.update(z)

# update zpools with properties
for zpoolname, z in zpoolpropdata.iteritems():
    z['name'] = zpoolname
    zpool.update(z, merge=True)

for zvolname, z in zfsvoldata.iteritems():
    z['name'] = zvolname
    z['zpoolname'] = zvolname.split('/')[0]
    if z['type'] == 'volume':
        zvol.update(z)
    elif z['type'] == 'snapshot':
        z['parent'] = zvolname.split('@')[0]
        zfssnapshot.update(z)

all_objs = dict(plugin.all_data())

import json

print json.dumps(all_objs, indent=2)

# pprint.pprint(target.data)
# pprint.pprint(targetportalgroup.data)
# pprint.pprint(hostgroup.data)
# pprint.pprint(view.data)
# pprint.pprint(lun.data)
# pprint.pprint(vdev.data)
# pprint.pprint(disk.data)
# pprint.pprint(cachedev.data)
# pprint.pprint(zildev.data)
# pprint.pprint(sparedev.data)
