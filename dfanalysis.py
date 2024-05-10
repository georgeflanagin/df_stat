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
import datetime
import getpass
import logging
import signal
import time

###
# Installed libraries
###
import pandas
import math
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
import fileutils
import linuxutils
from   urdecorators import show_exceptions_and_frames as trap
from   urlogger import URLogger
from   dfdata import DFStatsDB
from   dorunrun import dorunrun
from urmessage import send_urmessage
###
# Credits
###
__author__ = 'George Flanagin'
__copyright__ = 'Copyright 2022'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'George Flanagin'
__email__ = ['gflanagin@richmond.edu', 'me@georgeflanagin.com']
__status__ = 'in progress'
__license__ = 'MIT'

mynetid = getpass.getuser()
@trap
def analyze_diskspace_kpss(frame:pandas.DataFrame) -> None:
    """
    This method uses KPSS test to determine data non-stationarity.
    Data is non-stationary if
        1. Test Statistic > Critical Value
        2. p-value < 0.05
    If data is non-stationary, send email to hpc@richmond.edu
    """
    #columns : host   partition  used_space  free_space  error_code measured_at
    from dfstat import myconfig, logger
    if (frame["error_code"]==0).any():
        free_space = frame["free_space"][frame["error_code"]==0]
        used_space = frame["used_space"][frame["error_code"]==0]
        partition_size = frame["partition_size"][frame["error_code"]==0]
        mu, sigma = 0, 0.1
        statistic, p_value, n_lags, critical_values = kpss(free_space.astype(int), regression ='ct', store = True)
        #print(f'Result: The series is {"not " if p_value < 0.05 else ""}stationary')
        
        if (p_value < 0.05) and is_mem_drop(free_space):
            #initial_measurement = free_space.tolist()[0]
            #latest_measurement = free_space.tolist()[len(frame)-1]
            percent_occupied = (used_space / partition_size ) * 100
            return (True, percent_occupied) # alert
    return False           
 
@trap 
def analyze_diskspace_progressively(frame:pandas.DataFrame) -> None:
    from dfstat import myconfig, logger
    if (frame["error_code"]==0).any():
        free_space = frame["free_space"][frame["error_code"]==0].to_list()[len(frame)-1]
        used_space = frame["used_space"][frame["error_code"]==0].to_list()[len(frame)-1]
        partition_size = frame["partition_size"][frame["error_code"]==0].to_list()[len(frame)-1]
    try:
        # check the latest measurement

        # highest is greater than 20 Tb, taxed at 20%
        # mid is 2-20 Tb, taxed at 20%
        # lowest is less than 2 Tb, taxed at 10%
        thresholds = [2*1024*1024, 20*1024*1024]
        rates = [0.2, 0.1]
     
        taxed_val = 0
        if (partition_size < thresholds[0]) or (partition_size > thresholds[0] and partition_size < thresholds[1]):
            taxed_space = partition_size * 0.8
        elif used_space >  thresholds[1]:
            taxed_space = partition_size * 0.9
                    
        if used_space > taxed_space:
            percent_occupied = (used_space / partition_size ) * 100
            #print(free_space, "//", taxed_used_space)
            return (True, percent_occupied) # alert
    except Exception as e:
        return False
    return False 

@trap
def analyze_diskspace_changerate(frame:pandas.DataFrame) -> None:
    from dfstat import myconfig, logger
    if (frame["error_code"]==0).any():
        free_space = frame["free_space"][frame["error_code"]==0]
        used_space = frame["used_space"][frame["error_code"]==0].tolist()[len(frame)-1]
        partition_size = frame["partition_size"][frame["error_code"]==0].tolist()[len(frame)-1]
    try:
        print(free_space)

        initial_measurement = free_space.tolist()[0]
        latest_measurement = free_space.tolist()[len(frame)-1]
        
        rate_of_change = (latest_measurement - initial_measurement) / initial_measurement

        if rate_of_change > 0.5:
            percent_occupied = (used_space / partition_size ) * 100
            return (True, math.ceil(percent_occupied))
        return False
    except Exception as e:
        print(e)
        return False 

@trap
def send_email(subject:str):
    from dfstat import myconfig, logger
    cmd = f"nohup mailx -s {subject} '{myconfig.notification_address[0]}' /dev/null 2>&1 &"
    print(cmd)
    dorunrun(cmd)

@trap
def is_mem_drop(frame:pandas.DataFrame) -> bool:
    
    print(frame.tail(2))
    last_two_values = frame.tail(2).to_list()
    print("last two", last_two_values)

    mem_then = last_two_values[0]
    mem_now = last_two_values[1]
    if mem_then > mem_now:
        return True
    return False
@trap
def run_analysis() -> None:
    """
    We need to get the recent records, where recent is defined as
    more recent than a cutoff value related to the measurement
    interval. IOW, the last "N" values.
    """
    from dfstat import myconfig, logger, db
    print("run_analysis")
    logger.debug("run_analysis")
    seconds_ago =  myconfig.time_interval * myconfig.window_size
    starttime = timestamp_to_sqlite(time.time() - seconds_ago)
       
    SQL = f"""
        SELECT * from v_recent_measurements""" # where measured_at > "{starttime}"
        #"""
    frame = db.execute_SQL(SQL)
    
    for host_frame in [group for _, group in frame.groupby('host')]:
        for partition_frame in ( group for _, group in host_frame.groupby('partition') ):
            analysis = analyze_diskspace_changerate(partition_frame)
            #analysis = analyze_diskspace_kpss(partition_frame)
            #analysis = analyze_diskspace_progressively(partition_frame)
            print("analysis", analysis)
            if type(analysis) == tuple:
                if analysis[0] is True:
                    host = host_frame["host"].to_list()[0]
                    partition = host_frame["partition"].to_list()[0]
                     
                    subject = f"Check {partition} on {host}"
                    body = f"The diskspace is {(analysis[1])}% occupied."
                    print("about to send a message")
                    send_urmessage('alina.enikeeva@richmond.edu', 'test message', 'test body')
                    print(subject, body)   
            #analyze_diskspace_changerate(partition_frame)

@trap
def timestamp_to_sqlite(t:int) -> str:
    return datetime.datetime.utcfromtimestamp(int(t)).strftime('%Y-%m-%d %H:%M:%S')

@trap
def dfanalysis_main(myargs:argparse.Namespace=None) -> int:
    from dfstat import myconfig, logger, db
 
    print(f"{myconfig=} {logger=} {db=}")

    # Set up to ignore all signals, ....
    for sig in range(0, signal.SIGRTMAX):
        try:
            signal.signal(sig, SIGIGN)
        except:
            pass

    # Except for these:
    #for sig in [ signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM, signal.SIGQUIT, signal.SIGINT ]:
    #    signal.signal(sig, handler)

    #logfile=f"{os.path.basename(__file__)[:-3]}.log" 
    #logger = URLogger(logfile=logfile, level=logging.DEBUG)

    while True:
        print("here")
        run_analysis()
        print("there")
        time.sleep(myconfig.time_interval)

    return os.EX_OK


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="dfanalysis", 
        description="What dfanalysis does, dfanalysis does best.")

    parser.add_argument('-i', '--input', type=str, default="",
        help="Input file name.")
    parser.add_argument('-o', '--output', type=str, default="",
        help="Output file name")
    parser.add_argument('--nice', type=int, choices=range(0, 20), default=0,
        help="Niceness may affect execution time.")
    parser.add_argument('-v', '--verbose', action='store_true',
        help="Be chatty about what is taking place")

    myargs = parser.parse_args()
    myargs.verbose and linuxutils.dump_cmdline(myargs)
    if myargs.nice: os.nice(myargs.nice)

    try:
        outfile = sys.stdout if not myargs.output else open(myargs.output, 'w')
        with contextlib.redirect_stdout(outfile):
            sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")


