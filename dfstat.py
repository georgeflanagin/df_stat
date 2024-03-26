# -*- coding: utf-8 -*-
import typing
from   typing import *

###
# Credits
###
__author__ = 'Alina Enikeeva'
__copyright__ = 'Copyright 2023, University of Richmond'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'Alina Enikeeva'
__email__ = 'alina.enikeeva@richmond.edu'
__status__ = 'in progress'
__license__ = 'MIT'


###
# Standard imports, starting with os and sys
###
min_py = (3, 8)
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
from   datetime import datetime
import getpass
import regex as re
import signal
import sqlite3
import time

###
# Installed libraries
###
import numpy as np
import pandas as pd
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
import linuxutils
from   urlogger import URLogger
from   sqlitedb import SQLiteDB
from   urdecorators import trap

###
# imports that are a part of this project
###
from   dfdata import DFStatsDB
import sshconfig

###
# global objects
###
myconfig  = None
sshconfig = None
logger    = URLogger(
                logfile=f"{os.path.basename(__file__)[:-3]}.log", 
                level=logging.DEBUG
                )

@trap
def determine_stationarity():
    """
    This method uses KPSS test to determine data non-stationarity.
    Data is non-stationary if
        1. Test Statistic > Critical Value
        2. p-value < 0.05
    If data is non-stationary, send email to hpc@richmond.edu
    """
    info = extract_df()
    for dir, _ in info.items():
        ### Conduct test for each directory
        select_SQL = f"""SELECT * FROM df_stat WHERE directory = '{dir}';"""
        df = db.execute_SQL(select_SQL, we_have_pandas=True) 

        # specify regression as ct to determine that null hypothesis is 
        # that the data is stationary
        # add noise to data so that KPSS statistics calculates results 
        clean_data = df["memavail"].astype(float) 
        mu, sigma = 0, 0.1 
        noise = np.random.normal(mu, sigma, [1, df.shape[0]])
        data_with_noise = clean_data + noise[0]
        statistic, p_value, n_lags, critical_values = kpss(data_with_noise, regression ='ct', store = True)
    
        # debug output
        print(f'Result: The series is {"not " if p_value < 0.05 else ""}stationary')

        ### send email if data is non-stationary and if there is memory drop
        if (p_value < 0.05) and is_mem_drop(dir):
            subject = f"'Check {dir}, there might be a disk space drop.'"
            # send email in the background
            cmd = f"nohup mailx -s {subject}  '{myargs.email}' /dev/null 2>&1 &"
            dorunrun(cmd)
        #except:
        #    return
    return


@trap
def empty_generator()
    return
    yield


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
    
    d = {}
    for _0, space, used, available _1, partition in lines.split('\n'):
        if partition in partitions:
            d[partition] = [int(space), int(used), int(available)]
 
    return d
    
@trap
def fiscaldaemon_events() -> int:
    """
    This is the event loop.
    """
    global myargs, mylogger

    found_stop_event = False

    while True:
        determine_stationarity()
        time.sleep(myargs.timeinterval)
        insert_in_db()
        delete_older_entries()

    fileutils.fclose_all()
    mylogger.info('All files closed')
    return os.EX_OK

@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Map SIGHUP and SIGUSR1 to a restart/reload, and 
    SIGUSR2 and the other common signals to an orderly
    shutdown. 
    """
    global myconfig
    if signum in [ signal.SIGHUP, signal.SIGUSR1 ]: 
        dfstat_main(myconfig)

    elif signum in [ signal.SIGUSR2, signal.SIGQUIT, signal.SIGINT ]:
        logger.info('Closing up.')
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
    hostinfo = sshconfig[host]

    cmd = f"""
        ssh {hostinfo.user}.{hostinfo.hostname}:{hostinfo.port} 'df -P'
        """
    try: 
        result = SloppyTree(dorunrun(cmd, return_datatype = dict))
        if not result.OK:
            db.record_error(host, result.code)
            yield from empty_generator()

        # return the lines, minus the header row, and with the \n chopped off.
        yield from ( _.strip() for _ in result.stdout.split('\n')[1:] )

    except Exception as e:
        db.record_error(host, -1)
        yield from empty_generator()
    

@trap
def dfstat_main(myconfig:SloppyTree) -> int:
    """
    Note: passing myconfig as an argument is not necessary in the
        general case. It is a global. However, when we process
        a re-read or re-start signal, we want the handler to scoop
        up the global, and call this function. 
    """
    global sshconfig

    # Go demonic.
    here=os.getcwd()
    linuxutils.daemonize_me()
    os.chdir(here)

    # Read the ssh info. SSHConfig is derived from SloppyTree
    sshconfig = SSHConfig(myconfig.sshconfig_file)

    # Open the database.
    db = DFStatsDB(myconfig.database)

    while True:
        for host, partitions in db.get_targets():
            info = extract_df(query_host(host), partitions)

        time.sleep(myconfig.time_interval)



if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="dfstat", 
        description="What dfstat does, dfstat does best.")

    parser.add_argument('-i', '--input', type=str, default="dfstat.toml",
        help="toml file with the config info.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")

    myargs = parser.parse_args()

    for signal in [ signal.SIGINT, signal.SIGQUIT, signal.SIGHUP,
                        signal.SIGUSR1, signal.SIGUSR2, signal.SIGRTMIN+1 ]:
        signal.signal(signal, handler)

    try:
        with open(myargs.input, 'rb') as f:
            myconfig=SloppyTree(tomllib.load(f))
    except:
        print(f"Unable to read config from {myargs.input}")
        sys.exit(EX_CONFIG)
    
    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myconfig))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

