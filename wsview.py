# -*- coding: utf-8 -*-
"""
    This program will query a list of Linux computers for status
    of memory and CPU usage. 
"""


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
import fcntl
import logging
from   pprint import pprint
import pickle

###
# From hpclib
###
from   dorunrun import dorunrun
import fileutils
import linuxutils
from   sloppytree import SloppyTree
from   urdecorators import trap
import urlogger
from   urlogger import piddly


###
# imports and objects that are a part of this project
###
verbose = False
mynetid = getpass.getuser()

###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2023, University of Richmond'
__credits__ = 'Alina Enikeeva'
__version__ = 0.1
__maintainer__ = 'Alina Enikeeva, George Flanagin'
__email__ = 'hpc@richmond.edu'
__status__ = 'in progress'
__license__ = 'MIT'

DAT_FILE=os.path.join(os.getcwd(), 'info.dat')

@trap
def append_pickle(o:object, file_name:str) -> int:
    """
    Append a pickle of o to file_name.
    """
    global logger
    logger.info(piddly("append_pickle"))
    with open(file_name, 'ab+') as f:
        logger.info(piddly("Pickle file open, waiting for lock."))
        fcntl.lockf(f, fcntl.LOCK_EX)
        logger.info(piddly("locked."))
        try:
            pickle.dump(o, f)
            logger.info(piddly("Pickle written."))
        except pickle.PicklingError as e:
            logger.error(piddly(f"{e}"))
        finally:
            f.close()


@trap
def collectdata(host:str) -> int:
    """
    This is the function executed by the child process.
    """
    global logger, DAT_FILE
    logger.info(piddly(f"Collecting data about {host}"))
    try:
        append_pickle([host, 'stats', get_actual_stats(host)], DAT_FILE)
        append_pickle([host, 'mem', get_actual_mem_usage(host)], DAT_FILE)
        append_pickle([host, 'cores', get_actual_cores_usage(host)], DAT_FILE)

    except Exception as e:
        logger.error(piddly(f"{e}"))
        return os.EX_SOFTWARE

    return os.EX_OK

@trap
def extract_pickle(file_name:str) -> object:
    with open(file_name, 'rb') as f:
        while True:
            try:
                yield pickle.load(f)
            except EOFError:
                break

@trap
def get_actual_cores_usage(node:str) -> SloppyTree:
    """
    ssh to the node and get the number of cores that is actually used.
    """
    global logger
    logger.info(piddly(f"get_actual_cores_usage for {node}"))
    cmd = "ssh -o ConnectTimeout={} {} 'cat /proc/loadavg'"
    return parse_loadavg(dorunrun(cmd.format(myargs.wait_sec, node), return_datatype = str))

@trap 
def get_actual_mem_usage(node:str) -> SloppyTree:
    """
    ssh to the node and calculate how much memory is used.
    """
    global logger
    logger.info(piddly(f"get_actual_mem_usage for {node}"))
    cmd = "ssh -o ConnectTimeout={} {} 'cat /proc/meminfo | head -3'"
    return parse_meminfo(dorunrun(cmd.format(myargs.wait_sec, node), return_datatype = str))
   
@trap
def get_actual_stats(node:str) -> SloppyTree:
    """
    ssh to the node and retrive mpstat info.
    """
    global logger
    logger.info(piddly(f"get_actual_stats for {node}"))
    cmd = "ssh -o ConnectTimeout={} {} 'mpstat | tail -2'"
    return parse_mpstat(dorunrun(cmd.format(myargs.wait_sec, node), return_datatype=str))
    
@trap
def get_hostnames() -> tuple:
    """
    Gets the current list of workstations.
    """
    global myargs, logger
    logger.info("get_hostnames")
    try:
        hosts = tuple(_ for _ in fileutils.read_whitespace_file(myargs.input))
    except Exception as e:
        logger.error(f"Could not use {myargs.input} because {e}")
        sys.exit(os.EX_DATAERR)
    
    if not len(hosts):
        logger.error(f"No hosts found.")
        sys.exit(os.EX_DATAERR)

    return hosts

    
@trap
def get_info() -> SloppyTree:
    """
    Build a tree of the information we gather from the workstations.
    """
    global logger, DAT_FILE
    logger.info(piddly("Entered get_info"))

    t = SloppyTree()
    fork_ssh(get_hostnames())

    for p in extract_pickle(DAT_FILE):
        host, data_type, data_tree = p
        t[host][data_type] = data_tree

    return t

@trap
def parse_meminfo(s:str) -> SloppyTree:
    """
    Parse /proc/meminfo and return keys and values.
    """
    global logger
    d = {}
    for line in s.split('\n'):
        label, value, _ = line.split()
        try:
            d[label[:-1].lower()] = int(value)
        except Exception as e:
            d[label[:-1].lower()] = -1729

    return d


@trap
def parse_loadavg(s:str) -> SloppyTree:
    """
    Parse /proc/loadavg and return keys and values.
    """
    d = SloppyTree()

    d.minute_1, d.minute_5, d.minute_15, threads, _ = s.split()
    d.running_threads, d.total_threads = threads.split('/')
    for k, v in d.items():
        d[k] = float(v)

    return d

@trap
def parse_mpstat(s:str) -> SloppyTree:
    """
    Make sense of the mpstat output, and return keys and values.
    """

    d = SloppyTree()
    keys, values = s.split('\n')
    keys = keys.split()
    values = values.split()
    for k, v in dict(zip(keys, values)).items():
        if k.startswith('%'):
            d[k[1:]]=v

    return d
    
@trap
def fork_ssh(list_of_nodes:tuple) -> None:
    '''
    Multiprocesses to ssh to each node and retrieve this information 
    in parallel, significantly speeding up the process.
    '''
    global logger, myargs, DAT_FILE
    logger.info("Entered fork_ssh")
    
    try:
        os.unlink(DAT_FILE)
    except FileNotFoundError as e:
        # This is OK; the file has already been deleted.
        pass

    except Exception as e:
        # This is something else, let's find out what happened.
        logger.error(piddly(f"Cannot continue. Unable to clear {DAT_FILE} because {e}."))
        sys.exit(os.EX_IOERR)

    pids = set()

    for node in list_of_nodes:            

        # Parent process records the child's PID.
        if (pid := os.fork()):
            pids.add(pid)
            continue

        try:
            exit_code = 0
            exit_code = collectdata(node)
        finally:
            os._exit(exit_code)


    # make sure all the child processes finish before the function 
    # returns.
    while pids:
        # The zero in the argument means we wait for any child.
        # The resulting pid is the child that sent us a SIGCHLD.
        pid, exit_code, _ = os.wait3(0) 
        pids.remove(pid)


@trap
def wsview_main() -> int:
    #wrapper(draw_menu)
    global logger, myargs
    logger.info(piddly("Entered wsview_main"))
    

    data = dict(get_info())
    print(data)

    return os.EX_OK


if __name__ == '__main__':
    
    this_prog = os.path.basename(__file__)[:-3]
    logfile = f"{this_prog}.log"
    default_host_file = os.path.join(os.getcwd(), 'hosts')

    parser = argparse.ArgumentParser(prog="wsview", 
        description="What wsview does, wsview does best.")

    parser.add_argument('-c', '--columns', type=str,
        default="")
    parser.add_argument('-i', '--input', type=str, default=default_host_file,
        help=f"A whitespace delimited file of [user@]host names. Default file is {default_host_file}")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name. Defaults to stdout.")
    parser.add_argument('-w', '--wait-sec', type=int, default=1,
        help="Maximum seconds to wait for a response. The default is 1.")
    parser.add_argument('-v', '--verbose', type=int, default=logging.DEBUG, 
        help=f"Sets the loglevel. Values between {logging.NOTSET} and {logging.CRITICAL}. Default is {logging.DEBUG}")
    parser.add_argument('-z', '--zap-logfile', action='store_true',
        help="Remove old logfile if present.")

    myargs = parser.parse_args()
    verbose = ( myargs.verbose 
        if logging.NOTSET <= myargs.verbose <= logging.CRITICAL else 
        logging.DEBUG )
    if myargs.zap_logfile:
        try:
            os.unlink(logfile)
        except:
            pass

    logger = urlogger.URLogger(logfile=logfile, level=myargs.verbose)

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"]())

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

