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
from datetime import datetime
import getpass
import regex as re
import sqlite3
import time
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
from dorunrun import dorunrun
from sqlitedb import SQLiteDB

###
# imports that are a part of this project
###

###
# global objects
###
verbose = False

filesys = []

@trap
def run_df():
    """
    Returns result of df -h command.
    """
    cmd = "df -h"
    try: 
        return dorunrun(cmd, return_datatype = str)
    except Exception as e:
        #print(f"Could not run the program. Exception {e} occurred.")
        return None
    
def extract_df():
    """
    This command extracts values from df -h
    for filesystems.
    """
    d = {}
    mem_in_GB = 0
    lines = run_df().split('\n')
    for item in lines:
        for filesystem in filesys:
            word = [w for w in item.split()]
            if word[0] == filesystem:
                dir = word[5]

                # translate each memory entry to GB
                mem_entry = re.split('([a-zA-Z])+', word[3])
                mem_metric = mem_entry[1]
                mem_val = mem_entry[0]
                if mem_metric == "T":
                    mem_in_GB = float(mem_val) * 1000
                elif mem_metric == "G":
                    mem_in_GB = float(mem_val)
                elif mem_metric == "M":
                    mem_in_GB = float(mem_val) / 1000                
        
                d[dir] = mem_in_GB
    return d

def create_table():
    """
    Creates table in database with datetime, 
    directory and available memory columns.
    """
    sql_create_table = """CREATE TABLE IF NOT EXISTS df_stat(
                            datetime default current_timestamp,
                            directory varchar(10),
                            memavail varchar(5));"""
    db.execute_SQL(sql_create_table)
    
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
        #time.sleep(myargs.timeinterval)
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
        print(df["memavail"])
        statistic, p_value, n_lags, critical_values = kpss(df["memavail"], regression ='ct', store = True)
    
        # debug output
        #print(f'Result: The series is {"not " if p_value < 0.05 else ""}stationary')

        ### send email if data is non-stationary and if there is memory drop
        if (p_value < 0.05) and is_mem_drop(dir):
            subject = f"'Check {dir}, there might be a memory drop.'"
            # send email in the background
            cmd = f"nohup mailx -s {subject}  '{myargs.email}' /dev/null 2>&1 &"
            #dorunrun(cmd)
    return

@trap
def dfstat_main(myargs:argparse.Namespace) -> int:
    create_table()
    linuxutils.daemonize_me()
    insert_in_db()
    return fiscaldaemon_events()


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="dfstat", 
        description="What dfstat does, dfstat does best.")

    parser.add_argument('-i', '--input', type=str, default="filesys.txt",
        help="Input file with all file systems you need to track listed.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('-v', '--verbose', action='store_true',
        help="Be chatty about what is taking place")
    parser.add_argument('--db', type=str, default=os.path.realpath(f"dfstat.db"),
        help="Input database name.")
    parser.add_argument('-ti', '--timeinterval', type=int, default=60*60,
        help="Input time interval to update database.")
    parser.add_argument('--email', type=str, default="hpc@richmond.edu",
        help="Input email address to send a notification about memory drop.")



    myargs = parser.parse_args()
    verbose = myargs.verbose

    try:
        db = SQLiteDB(myargs.db)
    except:
        db = None
        print(f"Unable to open {myargs.db}")
        sys.exit(EX_CONFIG)
    
    with open(myargs.input) as f:
        filesys = f.read().split("\n")

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")

