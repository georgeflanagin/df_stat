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
import linuxutils
from   urdecorators import trap
from   dorunrun import dorunrun
from   sqlitedb import SQLiteDB

###
# imports that are a part of this project
###
from   dfdata import DFStatsDB

###
# global objects
###

@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Universal signal handler.
    """

    if signum in [ signal.SIGHUP, signal.SIGUSR1 ]: 
        tomb.tombstone('Rereading all configuration files.')
        

    elif signum in [ signal.SIGUSR2, signal.SIGQUIT, signal.SIGINT ]:
        tomb.tombstone('Closing up.')
        uu.fclose_all()
        sys.exit(os.EX_OK)

    elif signum == signal.SIGRTMIN+1:
        tomb.tombstone('Reloading code modules.')
        i, j = code.reload_code()
        tomb.tombstone('{} modules reloaded; {} new modules loaded.'.format(j, i))
        canoed()        

    else:
        tomb.tombstone(
            "ignoring signal {}. Check list of handled signals.".format(signum)
            )


@trap
def query_host(host:str) -> str:
    """
    Returns result of 'df -P' command. This POSIX option
    assures that different OS-es will return the data in the
    same POSIX format rather than the native format of the
    OS being queried.
    """
    cmd = f"ssh {host} 'df -P'"
    try: 
        result = SloppyTree(dorunrun(cmd, return_datatype = dict))
        if not result.OK:
            db.record_error(host, result.code)
            return ""

        # return the lines, minus the header row.
        return [ _.strip() for _ in result.stdout.split('\n')[1:] ]

    except Exception as e:
        db.record_error(host, -1)
        return ""
    

def extract_df(lines:list):
    """
    This command extracts values from df -P query. The parsed data 
    look something like this (but w/o the header).

    Filesystem                     1024-blocks      Used  Available Capacity Mounted on
    devtmpfs                              4096         0       4096       0% /dev
    tmpfs                             65486940        84   65486856       1% /dev/shm
    tmpfs                             26194780     59616   26135164       1% /run
    /dev/mapper/rl-root               73334784  16797908   56536876      23% /
    /dev/mapper/rl-home             1795845384 146868444 1648976940       9% /home
    /dev/sdb2                           983040    305856     677184      32% /boot
    /dev/sdb1                           613160      7144     606016       2% /boot/efi
    tmpfs                             13097388       328   13097060       1% /run/user/1000
    /dev/md125                      1343253684 135131388 1208122296      11% /oldhome
    141.166.186.35:/mnt/usrlocal/8  3766283008 263690368 3502592640       8% /usr/local/chem.sw
    tmpfs                             13097388        36   13097352       1% /run/user/1001
    """
    
    d = {}
    for line in lines:
        _0, space, _1, used, _2, partition = line.split()
        if 
 
    return d

def insert_in_db():
    """
    Inserts information into databse table.
    """
    SQL_insert = f"""INSERT INTO df_stat (directory, memavail) VALUES (?, ?)"""
    info = extract_df()
    for dir, mem in info.items():
        db.execute_SQL(SQL_insert, dir, mem)
    

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

def delete_older_entries():
    """
    Delete all the databse entries that are older than 7 days.
    """
    SQL_delete = """DELETE FROM df_stat WHERE datetime < DATETIME('now', '-1 day')"""
    db.execute_SQL(SQL_delete)

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
def dfstat_main(myconfig:SloppyTree) -> int:
    """
    Go demonic.
    Get the database open.
    Start querying.
    """
    here=os.getcwd()
    linuxutils.daemonize_me()
    os.chdir(here)

    db = DFStatsDB(myconfig.db)
    while True:
        for host, partitions in db.get_targets():

        time.sleep(myconfig.time_interval)



if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="dfstat", 
        description="What dfstat does, dfstat does best.")

    parser.add_argument('-i', '--input', type=str, default="dfstat.toml",
        help="toml file with the config info.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")

    myargs = parser.parse_args()

    caught_signals = [  signal.SIGINT, signal.SIGQUIT, signal.SIGHUP,
                        signal.SIGUSR1, signal.SIGUSR2, signal.SIGRTMIN+1 ]

    for signal in caught_signals:
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

