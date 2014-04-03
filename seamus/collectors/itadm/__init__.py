__author__ = 'achmed'

from .. import util

class ITAdmDataInterface(object):
    ITADM_BIN = '/usr/sbin/itadm'

    @classmethod
    def itadm_target_properties(cls):
        result = util._generic_command(cls.ITADM_BIN, 'list-target', '-v')
        lines = result.splitlines()

        # check header
        assert map(str.strip, lines[0].split()) == ['TARGET', 'NAME', 'STATE', 'SESSIONS'], 'unexpected fields in header'

        # parse target data
        itdata = cls._parse_itadm_targets(lines[1:])

        result = util._generic_command(cls.ITADM_BIN, 'list-tpg', '-v')
        lines = result.splitlines()

        # check header
        assert map(str.strip, lines[0].split()) == ['TARGET', 'PORTAL', 'GROUP', 'PORTAL', 'COUNT']

        # parse target portal data
        tpgdata = cls._parse_itadm_tpgs(lines[1:])

        return dict(targets=itdata, tpgs=tpgdata)

    @classmethod
    def _parse_itadm_tpgs(cls, lines):
        props = []

        portals = {}
        portalname = None

        def process_tpg():
            if len(props) > 0:
                assert portalname is not None, 'expected previous target id'
                portals[portalname] = {}
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
                    portals[portalname][propname] = propval

        for line in lines:
            if len(line.lstrip()) < len(line):
                props.append(map(str.strip, line.split(':', 1)))
            else:
                process_tpg()
                props = []
                portalname, count = map(str.strip, line.split())

        if len(props) > 0:
            process_tpg()

        return portals

    @classmethod
    def _parse_itadm_targets(cls, lines):
        props = []

        targets = {}
        targetiqn = None

        def process_target():
            if len(props) > 0:
                assert targetiqn is not None, 'expected previous target id'
                targets[targetiqn] = {}
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
                    targets[targetiqn][propname] = propval

        for line in lines:
            if len(line.lstrip()) < len(line):
                props.append(map(str.strip, line.split(':')))
            else:
                process_target()
                props = []
                targetiqn, state, sessions = map(str.strip, line.split())

        if len(props) > 0:
            process_target()

        return targets
