__author__ = 'achmed'

import subprocess

def _generic_command(cmd, *args):
    p = subprocess.Popen([cmd] + list(args), stdin=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    rc = p.returncode

    if rc != 0:
        raise Exception('%s returned unsuccessfully: %s' % (cmd, stderr,))

    return stdout
