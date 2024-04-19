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
def handler(signum:int, stack:object=None) -> None:
    """
    Map SIGHUP and SIGUSR1 to a restart/reload, and 
    SIGUSR2 and the other common signals to an orderly
    shutdown. 
    """
    global logger
    if signum in [ signal.SIGHUP, signal.SIGUSR1 ]: 
        dfanalysis_main()

    elif signum in [ signal.SIGUSR2, signal.SIGQUIT, signal.SIGINT ]:
        logger.info(f'Closing up from signal {signum}')
        fileutils.fclose_all()
        sys.exit(os.EX_OK)

    else:
        return

@trap
def analyze_diskspace(frame:pandas.DataFrame) -> None:
    pass


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
def fiscaldaemon_events() -> int:
    """
    This is the event loop.
    """
    global myargs, logger

    found_stop_event = False

    while True:
        determine_stationarity()
        time.sleep(myargs.timeinterval)
        insert_in_db()
        delete_older_entries()

    fileutils.fclose_all()
    logger.info('All files closed')
    return os.EX_OK


@trap
def run_analysis() -> None:
    """
    We need to get the recent records, where recent is defined as
    more recent than a cutoff value related to the measurement
    interval. IOW, the last "N" values.
    """
    from dfstat import myconfig, logger, db

    logger.debug("run_analysis")
    seconds_ago = myconfig.time_interval * myconfig.window_size
    starttime = timestamp_to_sqlite(time.time() - seconds_ago)
       
    SQL = f"""
        SELECT * from v_recent_measurements where measured_at > "{starttime}"
        """
    frame = pandas.read_sql(SQL, db())
    for host_frame in ( group for _, group in frame.groupby('host') ):
        for partition_frame in ( group for _, group in host_frame.groupby('partition') ):
            analyze_diskspace(partition_frame)

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
    for sig in [ signal.SIGHUP, signal.SIGUSR1, signal.SIGUSR2, signal.SIGTERM, signal.SIGQUIT, signal.SIGINT ]:
        signal.signal(sig, handler)

    logfile=f"{os.path.basename(__file__)[:-3]}.log" 
    logger = URLogger(logfile=logfile, level=logging.DEBUG)

    while True:
        run_analysis()
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


