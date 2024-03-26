# -*- coding: utf-8 -*-
"""
SSHConfig represents the object that contains ssh information.

t = SSHConfig('somefile')

t is a SloppyTree created by code in the sloppytree module of hpclib.
"""
import typing
from   typing import *

min_py = (3, 11)

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

###
# Installed libraries.
###


###
# From hpclib
###
import netutils
from   sloppytree import deepsloppy
from   urdecorators import *

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
__copyright__ = 'Copyright 2024'
__credits__ = None
__version__ = 0.1
__maintainer__ = 'George Flanagin'
__email__ = ['gflanagin@richmond.edu']
__status__ = 'in progress'
__license__ = 'MIT'

@singleton
class SSHConfig(dict):

    @trap
    def __init__(self, configfile:str):

        # Figure out how we are being called and if any of this will work
        # like it should. If called without any arguments, we are just
        # getting information that is already present.
        try:
            self._config = deepsloppy(netutils.get_ssh_host_info('all', configfile))
        except Exception as e:
            print(f"{e=}")
            sys.exit(os.EX_CONFIG)
         



if __name__ == "__main__":
    
    # Instantiate SSHConfig to load the configuration
    try:
        config_instance = SSHConfig(sys.argv[1])
    except:
        config_instance = SSHConfig('~/.ssh/config')

    t = config_instance
    print(t)
