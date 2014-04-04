__author__ = 'achmed'

import pyximport
pyximport.install()

import sys
import json

import jsonpath_rw

import pprint

import collections
import itertools
import operator

from timeit import default_timer

class Timer(object):
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.timer = default_timer

    def __enter__(self):
        self.start = self.timer()
        return self

    def __exit__(self, *args):
        end = self.timer()
        self.elapsed_secs = end - self.start
        self.elapsed = self.elapsed_secs * 1000  # millisecs
        if self.verbose:
            print 'elapsed time: %f ms' % self.elapsed


def node_label_mapper(o):
    nodetype = o['type']
    nodeplugin = o['pluginname']

    plugintypepathmap = {
        ('zfsnode','cachedev'): lambda v: v['_d']['devicename'],
        ('zfsnode','disk'): lambda v: v['_d']['devicename'],
        ('zfsnode','hostgroup'): lambda v: v['_d']['name'],
        ('zfsnode','logdev'): None,
        ('zfsnode','lun'): lambda v: v['_d']['luid'],
        ('zfsnode','sparedev'): lambda v: v['_d']['devicename'],
        ('zfsnode','target'): lambda v: 'server:%s' % v['_d']['target_iqn'],
        ('zfsnode','targetportalgroup'): lambda v: v['_d']['name'],
        ('zfsnode','vdev'): lambda v: v['_d']['devicename'],
        ('zfsnode','view'): lambda v: '%s:%d' % (v['_d']['luid'], v['_d']['lun']),
        ('zfsnode','zfssnapshot'): lambda v: v['_d']['name'],
        ('zfsnode','zpool'): lambda v: v['_d']['name'],
        ('zfsnode','zvol'): lambda v: v['_d']['name'],
        ('hvnode','lun'): lambda v: '%s:%d' % (v['_d']['target'], v['_d']['lun']),
        ('hvnode','target'): lambda v: 'client:%s' % v['_d']['target_iqn'],
    }

    return plugintypepathmap[(nodeplugin,nodetype)](o)

class Extractor(object):
    def __init__(self, extract_closure):
        self.extract_closure = extract_closure

    def __call__(self, o):
        try:
            return self.extract_closure(o)
        except Exception, e:
            return None

def extractor_mutator(extractor, mutator):
    def wrapper(f):
        def mutated_extractor(*args, **kwargs):
            return mutator(f(*args, **kwargs))
        return mutated_extractor
    extractor.extract_closure = wrapper(extractor.extract_closure)
    return extractor

class UnboundJsonPathExtractor(Extractor):
    def __init__(self, path):
        e = jsonpath_rw.parse(path)

        def extractor(o):
            v = e.find(o)
            if len(v) == 0:
                raise Exception('not found')
            else:
                return map(lambda p: p.value, v)

        super(UnboundJsonPathExtractor, self).__init__(extractor)

class BoundJsonPathExtractor(Extractor):
    def __init__(self, path, value):
        if not isinstance(value, collections.Sequence) or isinstance(value, (basestring, unicode)):
            value = [value]

        e = jsonpath_rw.parse(path)
        
        def matcher(o):
            v = e.find(o)
            resolvedval = map(lambda p: p.value, v)
            if resolvedval == value:
                return resolvedval
            raise Exception('no matchy')

        super(BoundJsonPathExtractor, self).__init__(matcher)

class RelationThingExtractor(object):
    def __init__(self, *extractors):
        self.extractors = extractors

    def makerelations(self, *objs):
        for oset in objs:
            for k,o in oset.iteritems():
                tupvalues = tuple(map(lambda x: x(o), self.extractors))
                if None not in tupvalues:
                    yield RelationThing(k, tupvalues)

class RelationThing(object):
    def __init__(self, objuuid, rels):
        self.objuuid = objuuid
        self.rels = rels

def relate(label, comparator, *relsets):
    assert len(relsets) >= 2

    for r in itertools.product(*relsets):
        if comparator(*r):
            yield tuple([label] + map(operator.attrgetter('objuuid'), r))
    
def main(*args):
    allobjs = {}

    for filename in args:
        with open(filename, 'r') as fh:
            d = json.load(fh)

        for objuuid, obj in d.iteritems():
            assert objuuid not in allobjs
            allobjs[objuuid] = obj

    zfsnode = BoundJsonPathExtractor('pluginname', 'zfsnode')
    viewtype = BoundJsonPathExtractor('type', 'view')
    luid = UnboundJsonPathExtractor('_d.luid')
    lun = UnboundJsonPathExtractor('_d.lun')

    viewrelation = RelationThingExtractor(zfsnode, viewtype, luid, lun)

    luntype = BoundJsonPathExtractor('type', 'lun')
    data_file = extractor_mutator(UnboundJsonPathExtractor('_d.data_file'), lambda o: [ o[0][len('/dev/zvol/rdsk/'):], ])
    
    lunrelation = RelationThingExtractor(zfsnode, luntype, luid, data_file)

    snapshottype = BoundJsonPathExtractor('type', 'zfssnapshot')
    parent = UnboundJsonPathExtractor('_d.parent')

    snapshotrelation = RelationThingExtractor(zfsnode, snapshottype, parent)

    zvoltype = BoundJsonPathExtractor('type', 'zvol')
    zpoolname = UnboundJsonPathExtractor('_d.zpoolname')
    whatevername = UnboundJsonPathExtractor('_d.name')

    zvolrelation = RelationThingExtractor(zfsnode, zvoltype, zpoolname, whatevername)
    
    zpooltype = BoundJsonPathExtractor('type', 'zpool')
    
    zpoolrelation = RelationThingExtractor(zfsnode, zpooltype, whatevername)

    vdevtype = BoundJsonPathExtractor('type', 'vdev')
    devicename = UnboundJsonPathExtractor('_d.devicename')
        
    vdevrelation = RelationThingExtractor(zfsnode, vdevtype, devicename, zpoolname)

    disktype = BoundJsonPathExtractor('type', 'disk')
    parentdev = UnboundJsonPathExtractor('_d.parentdev')

    diskrelation = RelationThingExtractor(zfsnode, disktype, parentdev, zpoolname)

    cachetype = BoundJsonPathExtractor('type', 'cachedev')

    cacherelation = RelationThingExtractor(zfsnode, cachetype, zpoolname)

    # logtype = BoundJsonPathExtractor('type', 'logdev')

    # logrelation = RelationThingExtractor(zfsnode, logtype, zpoolname)

    sparetype = BoundJsonPathExtractor('type', 'sparedev')

    sparerelation = RelationThingExtractor(zfsnode, sparetype, zpoolname)

    tpgtype = BoundJsonPathExtractor('type', 'targetportalgroup')
    portal = UnboundJsonPathExtractor('_d.portals')

    tpgrelation = RelationThingExtractor(zfsnode, tpgtype, whatevername, portal)

    hvnode = BoundJsonPathExtractor('pluginname', 'hvnode')
    targettype = BoundJsonPathExtractor('type', 'target')
    ifaceinitiator = UnboundJsonPathExtractor('_d.iface_initiatorname')
    current_portal = extractor_mutator(UnboundJsonPathExtractor('_d.current_portal'), lambda o: [ o[0].split(',', 1)[0], ] )
    target_iqn = UnboundJsonPathExtractor('_d.target_iqn')

    hvtargetrelation = RelationThingExtractor(hvnode, targettype, ifaceinitiator, target_iqn, current_portal)

    tpg = UnboundJsonPathExtractor('_d.targetportalgroup')

    zfstargetrelation = RelationThingExtractor(zfsnode, targettype, tpg, target_iqn)

    target = UnboundJsonPathExtractor('_d.target')

    hvlunrelation = RelationThingExtractor(hvnode, luntype, target, lun)

    with Timer() as t:
        viewrelations = list(viewrelation.makerelations(allobjs))
        lunrelations = list(lunrelation.makerelations(allobjs))
        zvolrelations = list(zvolrelation.makerelations(allobjs))
        snapshotrelations = list(snapshotrelation.makerelations(allobjs))
        zpoolrelations = list(zpoolrelation.makerelations(allobjs))
        vdevrelations = list(vdevrelation.makerelations(allobjs))
        diskrelations = list(diskrelation.makerelations(allobjs))
        cacherelations = list(cacherelation.makerelations(allobjs))
        # logrelations = list(logrelation.makerelations(allobjs))
        sparerelations = list(sparerelation.makerelations(allobjs))
        tpgrelations = list(tpgrelation.makerelations(allobjs))
        hvtargetrelations = list(hvtargetrelation.makerelations(allobjs))
        zfstargetrelations = list(zfstargetrelation.makerelations(allobjs))
        hvlunrelations = list(hvlunrelation.makerelations(allobjs))

    print 'elapsed %0.9fs' % t.elapsed_secs, 'collecting relation bits'

    with Timer() as t:
        lunviewedges = list(relate('viewOf', lambda r1, r2: r1.rels[2] == r2.rels[2], viewrelations, lunrelations))
        snapshotzvoledges = list(relate('snapshotOf', lambda r1, r2: r1.rels[2] == r2.rels[3], snapshotrelations, zvolrelations))
        zvollunedges = list(relate('lunOf', lambda r1, r2: r1.rels[3] == r2.rels[3], lunrelations, zvolrelations))
        zpoolzvoledges = list(relate('zpoolOf', lambda r1, r2: r1.rels[2] == r2.rels[2], zpoolrelations, zvolrelations))
        vdevzpooledges = list(relate('vdevIn', lambda r1, r2: r1.rels[3] == r2.rels[2], vdevrelations, zpoolrelations))
        diskvdevedges = list(relate('diskIn', lambda r1, r2: r1.rels[3] == r2.rels[3] and r1.rels[2] == r2.rels[2], diskrelations, vdevrelations))
        cachezpooledges = list(relate('zilFor', lambda r1, r2: r1.rels[2] == r2.rels[2], cacherelations, zpoolrelations))
        sparezpooledges = list(relate('spareFor', lambda r1, r2: r1.rels[2] == r2.rels[2], sparerelations, zpoolrelations))
        tpgtargetrelations = list(relate('tpgOf', lambda r1, r2: r1.rels[2] == r2.rels[2], tpgrelations, zfstargetrelations))
        portallevelconnection = list(relate('iscsiConnection', lambda r1, r2: r1.rels[4] == r2.rels[3], hvtargetrelations, tpgrelations))
        initiatorlevelconnection = list(relate('initiatorConnection',lambda r1, r2: r1.rels[3] == r2.rels[3], zfstargetrelations, hvtargetrelations))
        lunlevelconnection = list(relate('lunConnection', lambda r1, r2: r1.rels[3] == r2.rels[3], hvlunrelations, viewrelations))

        # lunviewedges = sorted(lunviewedges, key=operator.itemgetter(1))
        # for pair in lunviewedges:
        #     print pair[0], '->', pair[1]
        # v, l = lunviewedges[0]
        # print 'view\n' + json.dumps(allobjs[v], indent=2)
        # print 'lun\n' + json.dumps(allobjs[l], indent=2)

    print 'elapsed %0.9fs' % t.elapsed_secs, 'relating'

    print '-----relations-----'

    # for tup in viewrelations:
    #     print tup.objuuid, tup.rels

    # if len(viewrelations) > 0:
    #     print viewrelations[0].objuuid, viewrelations[0].rels
    print len(viewrelations), 'viewrelations'

    # if len(lunrelations) > 0:
    #     print lunrelations[0].objuuid, lunrelations[0].rels
    print len(lunrelations), 'lunrelations'

    # if len(snapshotrelations) > 0:
    #     print snapshotrelations[0].objuuid, snapshotrelations[0].rels
    print len(snapshotrelations), 'snapshotrelations'

    # if len(zvolrelations) > 0:
    #     print zvolrelations[0].objuuid, zvolrelations[0].rels
    print len(zvolrelations), 'zvolrelations'

    # if len(zpoolrelations) > 0:
    #     print zpoolrelations[0].objuuid, zpoolrelations[0].rels
    print len(zpoolrelations), 'zpoolrelations'

    # if len(vdevrelations) > 0:
    #     for v in vdevrelations:
    #         print v.objuuid, v.rels
    print len(vdevrelations), 'vdevrelations'

    # if len(diskrelations) > 0:
    #     for v in diskrelations:
    #         print v.objuuid, v.rels
    print len(diskrelations), 'diskrelations'

    # if len(cacherelations) > 0:
    #     for v in cacherelations:
    #         print v.objuuid, v.rels
    print len(cacherelations), 'cacherelations'

    # if len(logrelations) > 0:
    #     for v in logrelations:
    #         print v.objuuid, v.rels
    # print len(logrelations), 'logrelations'

    # if len(sparerelations) > 0:
    #     for v in sparerelations:
    #         print v.objuuid, v.rels
    print len(sparerelations), 'sparerelations'

    if len(tpgrelations) > 0:
        for v in tpgrelations:
            print v.objuuid, v.rels
    print len(tpgrelations), 'tpgrelations'

    if len(hvtargetrelations) > 0:
        for v in hvtargetrelations:
            print v.objuuid, v.rels
    print len(hvtargetrelations), 'hvtargetrelations'

    if len(zfstargetrelations) > 0:
        for v in zfstargetrelations:
            print v.objuuid, v.rels
    print len(zfstargetrelations), 'zfstargetrelations'

    # if len(hvlunrelations) > 0:
    #     for v in hvlunrelations:
    #         print v.objuuid, v.rels
    print len(hvlunrelations), 'hvlunrelations'

    print '-----edges-----'

    # if len(lunviewedges) > 0:
    #     print lunviewedges[0]
    print len(lunviewedges), 'view to lun edges'

    # if len(zvollunedges) > 0:
    #     print zvollunedges[0]
    print len(zvollunedges), 'zvol to lun edges'

    # if len(snapshotzvoledges) > 0:
    #     print snapshotzvoledges[0]
    print len(snapshotzvoledges), 'snapshot to zvol edges'

    # if len(zpoolzvoledges) > 0:
    #     print zpoolzvoledges[0]
    print len(zpoolzvoledges), 'zpool to zvol edges'

    # if len(vdevzpooledges) > 0:
    #     print vdevzpooledges[0]
    print len(vdevzpooledges), 'vdev to zpool edges'

    # if len(diskvdevedges) > 0:
    #     print diskvdevedges[0]
    print len(diskvdevedges), 'disk to vdev edges'

    # if len(cachezpooledges) > 0:
    #     print cachezpooledges[0]
    print len(cachezpooledges), 'cache to zpool edges'

    # if len(sparezpooledges) > 0:
    #     print sparezpooledges[0]
    print len(sparezpooledges), 'spare to zpool edges'

    if len(tpgtargetrelations) > 0:
        for v in tpgtargetrelations:
            print v
    print len(tpgtargetrelations), 'tpg target relation on zfs edges'

    # if len(portallevelconnection) > 0:
    #     print portale0levelconnection[0]
    print len(portallevelconnection), 'initiator target (at portal level) edges'

    if len(initiatorlevelconnection) > 0:
        print initiatorlevelconnection[0]
    print len(initiatorlevelconnection), 'initiator target (at target level) edges'

    if len(lunlevelconnection) > 0:
        print lunlevelconnection[0]
    print len(lunlevelconnection), 'lun to lun edges'

    import networkx as nx
    import matplotlib.pyplot as plt

    G = nx.DiGraph()

    # for k,v in allobjs.iteritems():
    #     attrs = v['_d']
    #     attrs = dict(filter(lambda t: not isinstance(t[1], (collections.Sequence, collections.Mapping)) or isinstance(t[1], (unicode, basestring)), attrs.iteritems()))
    #     G.add_node(k, **attrs)

    for k,v in allobjs.iteritems():
        nodetype = v['pluginname']
        nodeplugin = v['type']
        _type='%s:%s' % (nodetype, nodeplugin)
        G.add_node(k, type=_type)

    for set in lunviewedges, zvollunedges, zpoolzvoledges, snapshotzvoledges, vdevzpooledges, diskvdevedges, cachezpooledges, sparezpooledges, tpgtargetrelations, portallevelconnection, initiatorlevelconnection, initiatorlevelconnection, lunlevelconnection:
        if len(set) == 0:
            continue

        edgelabel = set[0][0]

        G.add_edges_from(map(lambda t: (t[1], t[2],), set), label=edgelabel)

    G = nx.relabel_nodes(G, {k: node_label_mapper(v) for k,v in allobjs.iteritems()})

    nx.draw(G)

    nx.write_graphml(G, '/tmp/wat.graphml')
    nx.write_gexf(G, '/tmp/wat.gexf')

    import networkx.readwrite.json_graph

    with open('/tmp/wat.json', 'w') as fh:
        json.dump(networkx.readwrite.json_graph.node_link_data(G), fh, indent=2)

    # plt.show()


if __name__ == '__main__':
    main(*tuple(sys.argv[1:]))

# vim: set ts=4 sw=4 expandtab:
