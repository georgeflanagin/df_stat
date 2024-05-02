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
import contextlib
import getpass

###
# Installed libraries like numpy, pandas, paramiko
###

###
# From hpclib
###
import linuxutils
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

import socket

from urmessage import send_urmessage

# Run the client function
if __name__ == '__main__':
    send_urmessage('gflanagi@richmond.edu', 'test message')

