# -*- coding: utf-8 -*-
import typing
from   typing import *

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
import collections
import contextlib
import getpass

###
# Installed libraries like numpy, pandas, paramiko
###
import pandas

###
# From hpclib
###
import linuxutils
from   sloppytree import SloppyTree
import sqlitedb
from   urdecorators import trap


###
# imports and objects that were written for this project.
###

###
# Global objects
###
verbose = False
mynetid = getpass.getuser()

###
# Credits
###
__author__ = mynetid
__copyright__ = 'Copyright 2024, University of Richmond'
__credits__ = None
__version__ = 0.1
__maintainer__ = mynetid
__email__ = f'{mynetid}@richmond.edu'
__status__ = 'in progress'
__license__ = 'MIT'


SQL = SloppyTree({
    'cleanup' : """
        DELETE FROM df_stat WHERE rowid NOT IN 
            (SELECT rowid FROM v_recent_measurements)
        """,
    'error' : """INSERT INTO df_stat (host, error_code) VALUES (?, ?)""",
    'recent' : """SELECT * FROM v_recent_measurements LIMIT ?""",
    'recent_by_host' : """SELECT * FROM v_recent_measurements WHERE host=? LIMIT ?""",
    'targets' : """SELECT * FROM v_hosts""",
    'measurement' : """INSERT INTO df_stat 
        (host, partition, partition_size, avail_disk ) VALUES (?, ?, ?, ?)"""
    })


class DFStatsDB:
    
    def __init__(db_name:str) -> None:
        """
        Build a class member for the database. This way,
        we get the benefit of any decorators or special
        properties of the database.
        """
        self.db = sqlitedb.SQLiteDB(db_name)


    def cleanup(window_size:int) -> int:
        return self.db.execute_SQL(SQL.cleanup)


    def recent_records(host:str, partition:str, window_size:int) -> pandas.DataFrame:
        """
        Get the recent data for the statistical analysis.
        """
        return ( self.db.execute_SQL(SQL.recent, window_size)
            if host == 'all' else 
            self.db.execute_SQL(SQL.recent_by_host, host, window_size) )


    def record_error(host:str, code:int) -> int:
        return self.db.execute_SQL(SQL.error, host, code)


    def record_measurement(host:str, 
            partition:Iterable, 
            size:Iterable, 
            free:Iterable) -> int:
        """
        Record one or more measurements. We will use size as the sentinel
        to determine whether we are recording one or more.
        """
        if isinstance(size, int):
            return self.db.execute_SQL(SQL.measurement, host, partition, size, free)
        else:
            return self.db.executemany_SQL(SQL.measurement, 
                zip(host, partition, size, free))


    def targets() -> dict:
        """
        Get a list of everything we need to monitor. For maximum
        ease of querying the target computers, the data are
        returned with host as the key and a tuple of partitions
        as the value.
        """
        data = self.db.execute_SQL(SQL.targets())
        organized_data = collections.defaultdict(list)
        for k, v in ( row for row in data.itertuples(index=False) ):
            organized_data[k].append(v)
        return organized_data
        

