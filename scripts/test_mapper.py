import sys

import Cython.Compiler.Options as Options
Options.cimport_from_pyx = True

import pyximport
pyximport.install()

import seamus.mapper

seamus.mapper.main(*tuple(sys.argv[1:]))
