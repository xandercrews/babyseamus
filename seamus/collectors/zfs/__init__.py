__author__ = 'achmed'

from .. import util

class ZFSDataInterface(object):
    ZPOOL_BIN = '/usr/sbin/zpool'
    ZFS_BIN = '/usr/sbin/zfs'

    @classmethod
    def zfs_volume_properties(cls):
        result = util._generic_command(cls.ZFS_BIN, 'list', '-t', 'volume', '-o', 'name')
        lines = result.splitlines()

        # check header
        assert map(str.strip, lines[0].split()) == ['NAME',], 'unexpected fields in header'

        volumes = []

        # collect names
        for line in lines[1:]:
            fields = map(str.strip, line.split())
            assert len(fields) == 1, 'unexpected fields in results'
            volumes.append(fields[0])

        # get properties
        result = util._generic_command(cls.ZFS_BIN, 'get', '-Hp', '-r', '-t', 'snapshot,volume', 'all', *volumes)
        lines = result.splitlines()

        return cls._parse_properties(lines, scriptmode=True)

    @classmethod
    def zpool_properties(cls):
        result = util._generic_command(cls.ZPOOL_BIN, 'get', 'all')
        lines = result.splitlines()

        return cls._parse_properties(lines)

    @classmethod
    def zpool_status(cls):
        result = util._generic_command(cls.ZPOOL_BIN, 'status')

        pools = {}

        # parse status result
        IN_PROPS = 0
        IN_CONFIG_HEADER = 1
        IN_CONFIG_BODY = 2
        IN_ERROR_PROPS = 3
        IN_POOL_DEVICES = 4

        ZIL_DEVICE = 0
        ARC_DEVICE = 1
        SPARE_DEVICE = 2

        config_pool_padding = None
        config_vdev_padding = None

        current_pool = None
        current_vdev = None

        current_device_type = None

        current_state = IN_PROPS

        try:
            for line in result.splitlines(True):
                line = line.replace('\t', ' ' * 8)
                if current_state == IN_PROPS:
                    if line.strip() == '':
                        pass
                    else:
                        section, value = map(str.strip, line.split(':', 1))

                        if section == 'pool':
                            current_pool = value
                            pools[current_pool] = dict(name=current_pool)
                        elif section == 'state':
                            assert current_pool is not None, 'invalid parser state- current pool should be known'
                            pools[current_pool]['state'] = value
                        elif section == 'scan':
                            assert current_pool is not None, 'invalid parser state- current pool should be known'
                            pools[current_pool]['scan'] = value
                        elif section == 'config':
                            current_state = IN_CONFIG_HEADER
                elif current_state == IN_CONFIG_HEADER:
                    if line.strip() == '':
                        pass
                    else:
                        assert map(str.strip, line.split()) == ['NAME', 'STATE', 'READ', 'WRITE', 'CKSUM'], 'unexpected fields in zpool status header'
                        if config_pool_padding is None:
                            config_pool_padding = len(line) - len(line.lstrip(' '))
                        current_state = IN_CONFIG_BODY
                elif current_state == IN_CONFIG_BODY:
                    padding_len = len(line) - len(line.lstrip(' '))
                    if padding_len == config_pool_padding:
                        pool, state, read, write, cksum = map(str.strip, line.split())
                        assert pool == current_pool, 'expected pool name is consistent in table and section header'
                        assert state == pools[current_pool]['state'], 'expected pool state is consistent in table and section header'
                        pools[current_pool]['errors'] = dict(read=read, write=write, cksum=cksum)
                        current_state = IN_POOL_DEVICES
                        current_device_type = None
                elif current_state == IN_POOL_DEVICES:
                    padding_len = len(line) - len(line.lstrip(' '))
                    if padding_len == config_pool_padding:
                        line = line.strip()
                        assert line in ('logs', 'cache', 'spares',), 'expected cache or log device description'
                        if line == 'logs':
                            current_device_type = ARC_DEVICE
                        elif line == 'cache':
                            current_device_type = ZIL_DEVICE
                        elif line == 'spares':
                            current_device_type = SPARE_DEVICE
                    elif line.strip() == '':
                        current_state = IN_ERROR_PROPS
                    else:
                        if config_vdev_padding is None:
                            config_vdev_padding = len(line) - len(line.lstrip())
                            assert config_vdev_padding > config_pool_padding, 'vdev indent is expected to be greater than pool indent'
                        if padding_len == config_vdev_padding:
                            assert current_pool is not None, 'expected pool is known'

                            if current_device_type == SPARE_DEVICE:
                                vdev, state = map(str.strip, line.split())
                            else:
                                vdev, state, read, write, cksum = map(str.strip, line.split())

                            if current_device_type is None:
                                if 'vdev' not in pools[current_pool]:
                                    pools[current_pool]['vdev'] = {}
                                pools[current_pool]['vdev'][vdev] = dict(devicename=vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            elif current_device_type == ARC_DEVICE:
                                if 'cache' not in pools[current_pool]:
                                    pools[current_pool]['cache'] = {}
                                pools[current_pool]['cache'][vdev] = dict(devicename=vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            elif current_device_type == ZIL_DEVICE:
                                if 'log' not in pools[current_pool]:
                                    pools[current_pool]['log'] = {}
                                pools[current_pool]['log'][vdev] = dict(devicename=vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            elif current_device_type == SPARE_DEVICE:
                                if 'spares' not in pools[current_pool]:
                                    pools[current_pool]['spares'] = {}
                                pools[current_pool]['spares'][vdev] = dict(devicename=vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            current_vdev = vdev
                        else:
                            assert current_device_type != SPARE_DEVICE, 'not expecting nested spare devices'
                            nested_vdev, state, read, write, cksum = map(str.strip, line.split())
                            assert current_pool is not None, 'expected pool is known'
                            assert current_vdev is not None, 'expected vdev is known'
                            if current_device_type is None:
                                assert current_vdev in pools[current_pool]['vdev'], 'expected current vdev present in pool'
                                if 'vdev' not in pools[current_pool]['vdev'][current_vdev]:
                                    pools[current_pool]['vdev'][current_vdev]['vdev'] = {}
                                pools[current_pool]['vdev'][current_vdev]['vdev'][nested_vdev] = dict(name=nested_vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            elif current_device_type == ARC_DEVICE:
                                assert 'cache' not in pools[current_pool], 'expected there is no previous cache/zil device'
                                pools[current_pool]['cache'][current_vdev] = dict(name=nested_vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                            elif current_device_type == ZIL_DEVICE:
                                assert 'log' not in pools[current_pool], 'expected there is no previous log/l2arc device'
                                pools[current_pool]['log'][current_vdev] = dict(name=nested_vdev, state=state, errors=dict(read=read, write=write, cksum=cksum))
                elif current_state == IN_ERROR_PROPS:
                    if line.strip().startswith('errors:'):
                        errorheader, value = line.split(':', 1)
                        assert current_pool is not None, 'expected pool is known'
                        pools[current_pool]['errors'] = value
                        current_state = IN_PROPS
                        current_pool = None
                        current_vdev = None
                    else:
                        assert line.strip() == '', 'expect line is empty if not error property'
        except Exception, e:
            raise

        return pools

    @staticmethod
    def _parse_properties(lines, scriptmode=False):
        # check header if not in script mode
        if not scriptmode:
            assert map(str.strip, lines[0].split()) == ['NAME', 'PROPERTY', 'VALUE', 'SOURCE',], 'unexpected fields in header'

        names = {}

        # collect properties
        if scriptmode:
            line_iter = iter(lines)
        else:
            line_iter = iter(lines[1:])

        for line in line_iter:
            if scriptmode:
                name, propname, propvalue, source = line.split('\t')
            else:
                name, propname, rhs = map(str.strip, line.split(None, 2))
                propvalue, source = map(str.strip, rhs.rsplit(None, 1))
            if name not in names:
                names[name] = { propname: propvalue }
            else:
                names[name][propname] = propvalue

        return names
