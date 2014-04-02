__author__ = 'achmed'

from .. import util

import xml.sax
import StringIO

class STMFExportXMLParser(xml.sax.ContentHandler):
    HOST_GROUP_PROPS = 0

    def startDocument(self):
        self.targets = {}
        self.luns = {}
        self.hgs = {}
        self.tgs = {}

        self.stack = []

    def endDocument(self):
        pass

    def startElement(self, name, attrs):
        if name == 'service':
            assert 'name' in attrs, 'expected service name'
            assert attrs['name'] == 'system/stmf', 'expected stmf service output'
        elif name == 'property_group':
            propname = attrs['name']
            self.stack.insert(0, propname)
        elif name == 'propval':
            self.stack.insert(0, dict(name=attrs['name'], value=[attrs['value']]))
        elif name == 'property':
            self.stack.insert(0, dict(name=attrs['name'], value=[]))
        elif name == 'value_node':
            self.stack[0]['value'].append(attrs['value'])

    def _processPropertyGroup(self, props):
        proptype = props.pop()

        if proptype == 'host_groups':
            self._processHostGroupProperties(props)
        elif proptype == 'target_groups':
            # not implemented
            pass
        elif proptype.startswith('view_entry-'):
            _, viewid, luid = proptype.split('-')
            self._processViewEntryProperties(viewid, luid, props)
        elif proptype.startswith('lu-'):
            _, luid = proptype.split('-')
            self._processLUEntryProperties(luid, props)

    def _processLUEntryProperties(self, luid, props):
        luid = str(luid)

        if luid not in self.luns:
            self.luns[luid] = {}

    def _processViewEntryProperties(self, viewid, luid, props):
        view = {}

        viewid = str(viewid)
        luid = str(luid)

        for prop in props:
            view[prop['name']] = prop['value']

        if luid not in self.luns:
            self.luns[luid] = {}

        if 'views' not in self.luns[luid]:
            self.luns[luid]['views'] = {}

        prettyview = dict(
            host_group=filter(lambda x: x, map(str, view['host_group'])),
            target_group=filter(lambda x: x, map(str, view['target_group'])),
            lun=int(str(view['lu_nbr'][0][12:16] + view['lu_nbr'][0][8:12] + view['lu_nbr'][0][4:8] + view['lu_nbr'][0][0:4]), 16)
        )

        self.luns[luid]['views'][viewid] = prettyview

    def _processHostGroupProperties(self, props):
        hgs = {}

        for prop in props:
            if prop['name'].endswith('-member_list') and prop['name'].startswith('group_name-'):
                _, membernum, _ = prop['name'].split('-')
                membernum = int(membernum)
                if not membernum in hgs:
                    hgs[membernum] = {}
                hgs[membernum].update({'members': prop['value']})
            elif prop['name'].startswith('group_name'):
                _, membernum = prop['name'].split('-')
                membernum = int(membernum)
                if not membernum in hgs:
                    hgs[membernum] = {}
                hgs[membernum].update({'name': prop['value'][0]})

        self.hgs = dict([(str(h['name']), { 'members': map(str, h['members']) },) for h in hgs.itervalues()])

    def endElement(self, name):
        if name == 'property_group':
            self._processPropertyGroup(self.stack)
            self.stack = []

    def characters(self, content):
        pass

    def getData(self):
        return dict(luns=self.luns, hgs=self.hgs, tgs=self.tgs)

class STMFDataInterface(object):
    STMF_BIN = '/usr/sbin/stmfadm'
    SVC_EXPORT_BIN = '/usr/sbin/svccfg'

    @classmethod
    def stmf_list_targets(cls):
        # start with export because it's the fastest way to get all the views
        results = util._generic_command(cls.SVC_EXPORT_BIN, 'export', '-a', 'stmf')

        resultbuffer = StringIO.StringIO(results)

        xmlparser = STMFExportXMLParser()

        parser = xml.sax.make_parser()
        parser.setContentHandler(xmlparser)
        parser.parse(resultbuffer)

        stmfdata = xmlparser.getData()

        # enrich with lun data
        results = util._generic_command(cls.STMF_BIN, 'list-lu', '-v')
        luns = cls._parse_stmf_targets(results.splitlines())

        for lu,v in luns.iteritems():
            if lu not in stmfdata['luns']:
                stmfdata['luns'][lu] = {}
            stmfdata['luns'][lu].update(v)

        return stmfdata

    @classmethod
    def _parse_stmf_targets(cls, lines):
        props = []

        luns = {}
        luid = None

        def process_lu():
            if len(props) > 0:
                assert luid is not None, 'expected previous lun id'
                luns[luid] = {}
                for prop in props:
                    propname, propval = prop
                    if not propval:
                        propval = None
                    else:
                        try:
                            propval = int(propval)
                        except ValueError:
                            pass
                    propname = propname.replace(' ', '_').lower()
                    luns[luid][propname] = propval

        for line in lines:
            if line.startswith('LU Name:'):
                process_lu()
                props = []
                _, luid = map(str.strip, line.split(':'))
            else:
                props.append(map(str.strip, line.split(':')))


        if len(props) > 0:
            process_lu()

        return luns
