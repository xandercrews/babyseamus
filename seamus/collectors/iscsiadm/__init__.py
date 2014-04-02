__author__ = 'achmed'

from .. import util

class IScsiAdmDataInterface(object):
    ISCSIADM_BIN = '/usr/bin/iscsiadm'

    @classmethod
    def iscsiadm_session_properties(cls):
        result = util._generic_command(cls.ISCSIADM_BIN, '-m', 'session', '-P', '3')
        lines = result.splitlines()

        return cls._process_iscsiadm_properties(lines)

    @classmethod
    def _process_iscsiadm_properties(cls, lines):
        IN_HEADER = 0
        IN_DEVICES = 1

        state = IN_HEADER

        sessions = {}
        targetiqn = None
        current_lun =  None

        # TODO handle transition back to
        for line in lines:
            try:
                key, val = map(str.strip, line.split(':', 1))

                if state == IN_DEVICES:
                    if key.startswith('Host Number'):
                        key1, host_number, key2, host_state = line.strip().replace('Host Number', 'Host_Number').split()
                        sessions[targetiqn]['host_number'] = host_number
                        sessions[targetiqn]['host_state'] = host_state
                    elif key.startswith('scsi'):
                        devicenum, _, scsichannel, _, scsiid, _, lun = line.strip().split()
                        current_lun = int(lun)
                        if 'luns' not in sessions[targetiqn]:
                            sessions[targetiqn]['luns'] = {}
                        assert lun not in sessions[targetiqn]['luns']
                        sessions[targetiqn]['devicenum'] = devicenum
                        sessions[targetiqn]['luns'].update({current_lun: dict(lun=current_lun, scsiaddr='%d:%d' % (int(scsichannel), int(scsiid)))})
                    elif key.startswith('Attached scsi disk'):
                        assert current_lun is not None
                        assert current_lun in sessions[targetiqn]['luns']
                        _, _, _, diskdev, _, diskstate = line.strip().split()
                        sessions[targetiqn]['luns'][current_lun].update(dict(diskdev=diskdev, diskstate=diskstate))
                elif key == 'Target':
                    targetiqn = val
                    sessions[targetiqn] = {}
                elif key == 'Current Portal':
                    sessions[targetiqn]['current_portal'] = val
                elif key == 'Persistent Portal':
                    sessions[targetiqn]['persistent_portal'] = val
                elif key == 'Iface Name':
                    sessions[targetiqn]['iface_name'] = val
                elif key == 'Iface Transport':
                    sessions[targetiqn]['iface_transport'] = val
                elif key == 'Iface Initiatorname':
                    sessions[targetiqn]['iface_initiatorname'] = val
                elif key == 'iSCSI Connection State':
                    sessions[targetiqn]['iscsi_connection_state'] = val
                elif key == 'iSCSI Session State':
                    sessions[targetiqn]['iscsi_session_state'] = val
                elif key == 'Attached SCSI devices':
                    state = IN_DEVICES
            except:
                pass

        return sessions


