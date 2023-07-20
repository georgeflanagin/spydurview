# -*- coding: utf-8 -*-
import typing
from   typing import *

min_py = (3, 9)

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

###
# Installed libraries.
###


###
# From hpclib
###
import linuxutils
from   urdecorators import trap

###
# imports and objects that are a part of this project
###


###
# Global objects and initializations
###
verbose = False

###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2023'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'George Flanagin'
__email__ = ['gflanagin@richmond.edu']
__status__ = 'in progress'
__license__ = 'MIT'


@trap
def row(used:int, max_avail:int, scale:int=80, x:str="X", _:str="_", ends=('[', ']')) -> str:
    """
    used -- quantity to be filled with x.
    max_avail -- set of which used is a subset.
    scale -- if scale < max_avail, then used and max_avail are divided by scale.
    x -- the char used to show in-use-ness.
    _ -- the char used to show not-in-use-ness.
    ends -- decorators for start/finish.
    """
    try:
        used=int(used)
        max_avail=int(max_avail)
        scale=int(scale)
    except:
        raise Exception("numeric quantities are required")
    
    if not len(x) * len(_) * scale * max_avail:
        raise Exception("Cannot use zero length delimiters")

    if used < 0 or max_avail < 0 or scale < 0:
        raise Exception("quantities must be non-negative")

    used = max_avail if used > max_avail else used

    if scale < max_avail:
        used = round(used * scale / max_avail)
    else:
        scale = max_avail

    xes = used*x
    _s  = (scale-used)*_

    return f"{ends[0]}{xes}{_s}{ends[1]}"


    

@trap
def scaling_main(myargs:argparse.Namespace) -> int:
    args = ( 
        (0, 40),
        (0, 200),
        (250, 200),
        (50, 384),
        (768, 1500)
        )

    for pair in args:
        print(row(pair[0], pair[1]))

    return os.EX_OK


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="scaling", 
        description="What scaling does, scaling does best.")

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

