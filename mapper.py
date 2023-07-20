# -*- coding: utf-8 -*-
import typing
from   typing import *

min_py = (3, 8)

###
# Standard imports, starting with os and sys
###
import os
import sys
if sys.version_info < min_py:
    print(f"This program requires Python {min_py[0]}.{min_py[1]}, or higher.")
    sys.exit(os.EX_SOFTWARE)

###
# Other standard distro imports
###
import argparse
import contextlib
import getpass
mynetid = getpass.getuser()
import pprint

###
# From hpclib
###
from   dorunrun import dorunrun
import linuxutils
from   sloppytree import SloppyTree
from   urdecorators import trap

###
# imports and objects that are a part of this project
###
import scaling


verbose = False

###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2023, University of Richmond'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'George Flanagin, Alina Enikeeva'
__email__ = ['gflanagin@richmond.edu', 'alina.enikeeva@richmond.edu']
__status__ = 'in progress'
__license__ = 'MIT'

@trap
def draw_map() -> dict:

    scaling_values = {
        384000 : 25,
        768000 : 50,
        1536000 : 100
        }

    data = SeekINFO()
    memory_map = []
    core_map = []
   
    # We don't need the header row here is an example line:
    #
    # spdr12 424105 768000 up 52 12/40/0/52

    for line in ( _ for _ in data.stdout.split('\n')[1:] if _ ):
        node, free, total, status, true_cores, cores = line.split()
        cores = cores.split('/')
        used = int(total) - int(free)
        scale=scaling_values[int(total)]
        memory_map.append(f"{node} {scaling.row(used, total, scale)}")
        core_map.append(f"{node} {scaling.row(cores[1], true_cores)}")

    return {"memory":memory_map, "cores":core_map}

@trap
def SeekINFO() -> tuple:
    cmd = 'sinfo -o "%n %e %m %t %c %C"'
    data = SloppyTree(dorunrun(cmd, return_datatype=dict))
    
    if not data.OK:
        verbose and print(f"sinfo failed: {data.code=}")
        return os.EX_DATAERR

    verbose and print(data.stdout)
    return data

                
@trap
def mapper_main(myargs:argparse.Namespace) -> int:
    for k, v in draw_map().items():
        print(f"Map for {k}")
        v="\n".join(sorted(v))
        print(f'{v}')
        print()
    
    return os.EX_OK


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="mapper", 
        description="What mapper does, mapper does best.")

    parser.add_argument('-i', '--input', type=str, default="",
        help="Input file name.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('-v', '--verbose', action='store_true',
        help="Be chatty about what is taking place")


    myargs = parser.parse_args()
    verbose = myargs.verbose

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

