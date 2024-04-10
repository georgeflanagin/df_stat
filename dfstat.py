# -*- coding: utf-8 -*-
import typing
from   typing import *

###
# Credits
###
__author__ = 'Alina Enikeeva'
__copyright__ = 'Copyright 2023, University of Richmond'
__credits__ = None
__version__ = 0.2
__maintainer__ = 'George Flanagin'
__email__ = 'gflanagin@richmond.edu'
__status__ = 'in progress'
__license__ = 'MIT'


###
# Standard imports, starting with os and sys
###
min_py = (3, 11)
import os
import sys
if sys.version_info < min_py:
    print(f"This program requires Python {min_py[0]}.{min_py[1]}, or higher.")
    sys.exit(os.EX_SOFTWARE)

###
# Other standard distro imports
###
import argparse
from   collections.abc import Generator
import contextlib
import getpass
import logging
import re
import signal
import sqlite3
import time
import tomllib

###
# Installed libraries
###
import numpy as np
import statsmodels.api as sm
# Use Kwiatkowski-Phillips-Schmidt-Shin (KPSS) test
# to determine if the data is stationary
# if p-value of the test is less than 0.05, then
# the data isn't stationary (it has significant changes)
# and, hence, hpc@richmond.edu needs to be informed.
from statsmodels.tsa.stattools import kpss
mynetid = getpass.getuser()
from statsmodels.tsa.stattools import adfuller

###
# From hpclib
###
from   dorunrun import dorunrun
import fileutils
import linuxutils
from   sloppytree import SloppyTree
from   sqlitedb import SQLiteDB
from   urdecorators import trap
from   urlogger import URLogger

###
# imports that are a part of this project
###
from   dfanalysis import dfanalysis_main
from   dfdata import DFStatsDB
from   sshconfig import SSHConfig

###
# global objects
###
myconfig  = None
sshconfig = None
logger    = None
db        = None
my_kids   = set()

@trap
def extract_df(lines:list, partitions:list) -> object:
    """
    This command extracts values from df -P query. The unparsed data 
    look something like these data rows (but w/o the header).

    Filesystem                     1024-blocks      Used  Available Capacity Mounted on
    /dev/mapper/rl-root               73334784  16797908   56536876      23% /
    /dev/mapper/rl-home             1795845384 146868444 1648976940       9% /home
    /dev/md125                      1343253684 135131388 1208122296      11% /oldhome
    141.166.186.35:/mnt/usrlocal/8  3766283008 263690368 3502592640       8% /usr/local/chem.sw
    """
    global logger
    logger.debug("extract_df")
    d = {}
    for line in lines:
        _0, space, used, available, _1, partition = line.split()
        logger.debug(f"{partition=}")
        if partition in partitions:
            d[partition] = [int(space), int(used), int(available)]
            print(f'{partition=} {space=} {used=} {available=}')

    return d
    

@trap
def graceful_exit() -> int:
    """
    Close everything, and leave.
    """
    global my_kids, db

    ###
    # The first thing we do, let's kill all the children.
    #         -- Dick the Butcher, King Henry VI
    ###
    for pid in my_kids:
        try:
            os.kill(pid, signal.SIGKILL)
        except:
            pass

    try:
        db.close()
    except:
        pass

    fileutils.fclose_all()
    if os.isatty(1): return os.EX_OK
    os._exit(os.EX_OK)


@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Map SIGHUP and SIGUSR1 to a restart/reload, and 
    SIGUSR2 and the other common signals to an orderly
    shutdown. 
    """
    global logger
    logger.debug("handler")
    global myconfig
    if signum in [ signal.SIGHUP, signal.SIGUSR1 ]: 
        dfstat_main(myconfig)

    elif signum in [ signal.SIGUSR2, signal.SIGQUIT, signal.SIGINT ]:
        logger.info(f'Closing up from signal {signum}')
        fileutils.fclose_all()
        sys.exit(os.EX_OK)

    else:
        return

@trap
def is_mem_drop(dir: str) -> bool:
    """
    Compare last two entries for filesystem 
    and return True if the previous entry is higher than the 
    current one - f(then) > f(now). That way we will determine that 
    there is a drop in memory rather than the rise. 
    """
    SQL_select = f"""SELECT memavail FROM df_stat WHERE directory = '{dir}' ORDER BY datetime DESC LIMIT 2"""
    result = db.execute_SQL(SQL_select, we_have_pandas=True)
    mem_then = result.values[0][0]
    mem_now = result.values[1][0]
    if mem_then > mem_now:
        return True


@trap
def query_host(host:str) -> str:
    """
    Returns result of 'df -P' command. This POSIX option
    assures that different OS-es will return the data in the
    same POSIX format rather than the native format of the
    OS being queried.
    """
    global sshconfig
    global db
    global logger
    logger.debug(f"query_host {host=}")
    hostinfo = SloppyTree(sshconfig[host])
    logger.debug(f"{hostinfo=}")
    cmd = f"""
        ssh {hostinfo.user}@{hostinfo.hostname} 'df -P'
        """
    try: 
        result = SloppyTree(dorunrun(cmd, return_datatype = dict))
        
        if not result.OK:
            logger.error(f"{result=}")
            db.record_error(host, result.code)
            return []

        # return the lines, minus the header row, and with the \n chopped off.
        result = [ _.strip() for _ in result.stdout.split('\n')[1:] ]
        logger.debug("{result=}")
        return result

    except Exception as e:
        db.record_error(host, -1)
        logger.error(f"{e=}")
        return []
    

@trap
def null_generator():
    return
    yield


@trap
def dfstat_main(myconfig:SloppyTree) -> int:
    """
    Note: passing myconfig as an argument is not necessary in the
        general case. It is a global. However, when we process
        a re-read or re-start signal, we want the handler to scoop
        up the global, and call this function. 
    """
    global sshconfig
    global db
    global logger
    logger.debug("main")

    # Read the ssh info. SSHConfig is derived from SloppyTree
    try:
        sshconfig = SSHConfig(fileutils.expandall(myconfig.sshconfig_file))()
    except Exception as e:
        logger.error(f'{e=} Cannot open {myconfig.sshconfig_file}')
        sys.exit(os.EX_CONFIG)
    else:
        logger.debug(f"{sshconfig=}")

    # Open the database.
    # try:
    #     db = DFStatsDB(myconfig.database)
    # except Exception as e:
    #     print(f"{e=}")
    #     sys.exit(os.EX_CONFIG)
    db = DFStatsDB(myconfig.database)
    
    # Kick off the analyzer
    if not (pid := os.fork()):
        dfanalysis_main()

    my_kids.add(pid)

    try:
        while True:
            for host, partitions in db.targets.items():
                logger.debug(f"{host=} {partitions=}")
                info = extract_df(query_host(host), partitions)
                for partition, values in info.items():
                    db.record_measurement(host, partition, values[1], values[2])

            time.sleep(myconfig.time_interval)
    finally:
        return graceful_exit()
        


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="dfstat", 
        description="What dfstat does, dfstat does best.")

    parser.add_argument('-i', '--input', type=str, default="dfstat.toml",
        help="toml file with the config info.")
    parser.add_argument('--sql', type=str, default="dfstat.sql",
        help="sql statements to populate the database.")
    parser.add_argument('--no-daemon', action='store_true',
        help="run in the foreground (aids debugging)")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('-z', '--zap', action='store_true',
        help="remove old logfile.")

    myargs = parser.parse_args()
    logfile=f"{os.path.basename(__file__)[:-3]}.log" 

    for sig in [ signal.SIGINT, signal.SIGQUIT, signal.SIGHUP,
                        signal.SIGUSR1, signal.SIGUSR2, signal.SIGRTMIN+1 ]:
        signal.signal(sig, handler)

    try:
        with open(myargs.input, 'rb') as f:
            myconfig=SloppyTree(tomllib.load(f))
    except Exception as e:
        print(f"{e=}\nUnable to read config from {myargs.input}")
        sys.exit(os.EX_CONFIG)
    
    # Go demonic unless we decide not to.
    if not myargs.no_daemon:
        here=os.getcwd()
        linuxutils.daemonize_me()
        os.chdir(here)
    else:
        print(f"{os.getpid()=}")

        

    # Open the logfile here to avoid loss of the filehandle when going
    # daemonic. The correct process will now create the logfile.
    if myargs.zap:
        try:
            os.unlink(logfile)
        except:
            pass

    logger = URLogger(logfile=logfile, level=logging.DEBUG)

    logger.info('+++ BEGIN +++')

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myconfig))

    except Exception as e:
        logger.error(f"Escaped or re-raised exception: {e=}")

