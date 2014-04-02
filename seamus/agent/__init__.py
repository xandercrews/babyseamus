__author__ = 'achmed'

import jsonpath_rw
import inspect
import time

import operator
import functools

# a quick recursive dict merge from the interwebs
import collections

import uuid

def update(d, *others):
    for u in others:
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                r = update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
    return d

# a plugin shall be instantiated with name
# the plugin shall provide a factory for creating new 'types'.  a type is something like a vm, or a lun
# the type shall be provided an attrgetter which retrieves an index.  it can be based on a jsonpath

class IndexResolveException(Exception):
    pass

# what do i call this thing?
class plugin_type(object):
    def add_indexes(self, **indices):
        assert hasattr(self, 'indices'), 'expected the type was constructed with indices'
        plugin.add_indexes_to(self.indices, **indices)

    def get_by_uuid(self, uuid):
        if uuid in self.data:
            return self.data[uuid]
        else:
            return None

    def get_by_index(self, idx):
        if idx in self.index_data:
            obj_uuid = self.index_data[idx]
            return self.data[obj_uuid]
        else:
            return None

    def update(self, dct, envdata=None, merge=False):
        # generate compound index from data
        idx = []
        for i in self.indices.values():
            idx.append(i(dct))

        try:
            idx = tuple(map(str, idx))
        except Exception, e:
            raise IndexResolveException('could not resolve index')

        # look up the record by index
        if idx in self.index_data:
            obj_uuid = self.index_data[idx]
        else:
            # if no other record is indexed, create a new object and index it
            obj_uuid = uuid.uuid4().bytes
            self.index_data[idx] = obj_uuid

        e = self.plugin.envelope(type=self.plugintype)

        if obj_uuid in self.data and merge:
            e['_d'] = update({}, self.data[obj_uuid]['_d'], dct)
        else:
            e['_d'] = dct
        self.data[obj_uuid] = e

        return obj_uuid

class plugin(object):
    def __init__(self, pluginname):
        self.pluginname = pluginname

    def envelope(self, **envdata):
        # we'll generate the timestamp in case it's not supplied
        if 'timestamp' not in envdata:
            timestamp = time.time()
        else:
            timestamp = None

        e = update({}, dict(timestamp=timestamp, pluginname=self.pluginname), envdata)

        return e

    @staticmethod
    def jsonpath_matcher(e, d):
        """jsonpath expression (in AST, sorry): %s""" % str(e)
        results = e.find(d)
        len_results = len(results)
        if len_results > 1:
            raise IndexResolveException('more than one index value returned for object')
        elif len_results < 1:
            raise IndexResolveException('no index values found for object')
        return results[0].value

    @classmethod
    def add_indexes_to(cls, dct, **indices):
        new_indices = {}
        for k, v in sorted(indices.items(), key=operator.itemgetter(0)):
            if isinstance(v, (basestring, unicode)):
                jsonpath_expr = jsonpath_rw.parse(v)


                new_indices[k] = functools.partial(cls.jsonpath_matcher, jsonpath_expr)
            elif not inspect.isfunction(v):
                raise Exception('provided index \'%s\' is not a function, closure or method' % k)
            else:
                new_indices[k] = v

        dct.update(new_indices)

    def make_type(self, typename, **indices):
        new_indices = {}
        self.add_indexes_to(new_indices, **indices)
        dct = dict(plugintype=typename, indices=new_indices, index_data={}, data={}, plugin=self)
        return type(typename, (plugin_type, object,), dct)
