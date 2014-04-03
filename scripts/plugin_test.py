__author__ = 'achmed'

import seamus.agent

p = seamus.agent.plugin('hvnode')
lun = p.make_type('lun', id='luid')()

# print lun
# print lun.__class__
# print dir(lun)
# print lun.indices
#
# print lun.indices['id']({'luid': 5})
#
# lun.add_indexes(wat='wat')
#
# print lun.indices['wat']({'wat': 6})
# print lun.indices['wat']({'wat2': 6})

lun.update({'luid': 'somesuch', 'luid2': 'somesuch2', 'a': 1})
lun.update({'luid': 'somesuch', 'b': 1})
lun.update({'luid': 'somesuch', 'c': 1}, merge=True)
lun.update({'luid': 'somesuch', 'b': 2}, merge=True)

print lun.get_by_index(('somesuch',))
