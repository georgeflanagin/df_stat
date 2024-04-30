# -*- coding: utf-8 -*-
import typing
from   typing import *
from   collections.abc import Iterable

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
import getpass
import logging
import signal
import socket
import threading


###
# Installed libraries like numpy, pandas, paramiko
###

###
# From hpclib
###
from   dorunrun import dorunrun
import fileutils
import linuxutils
from   sloppytree import SloppyTree
from   urdecorators import trap
from   urlogger import URLogger
import tomllib

###
# imports and objects that were written for this project.
###

###
# Global objects
###
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

###
# This is provided for writers of messages. For example,
#   from urmessage import format_message
###
format_message = lambda destination, subject : f"${destination}#{subject}#{message}$"

@trap
def handler(signum:int, stack:object=None) -> None:
    """
    Map SIGHUP and SIGUSR1 to a restart/reload, and 
    SIGUSR2 and the other common signals to an orderly
    shutdown. 
    """
    global logger
    logger.info(f"Received signal {signum}.")
    fileutils.fclose_all()
    sys.exit(os.EX_OK)


def handle_message(client_socket:object, addr:str) -> None:
    data = b''
    try:
        print(f"Connected to {addr}")
        while True:
            if not (datum := client_socket.recv(4096)): break
            data += datum

        send_message(data.decode())

    finally:
        client_socket.close()


def send_message(message:str) -> None:
    global logger    

    if message:
        logger.info(message)     
    else:
        logger.info("no message")
        return       

    # check for boundaries.
    if message[0] != '$' or message[-1] != '$':
        logger.error('Malformed message.')
        return

    # check for correctness again.        
    try:
        destination, subject = message[1:-1].split('#')
    except Exception as e:
        logger.error(f'Malformed message. {e=}')
        return

    cmd = f"sudo mailx -s '{subject}' {destination} "
    logger.info(f"{cmd=}")
    result = dorunrun(cmd, return_datatype=dict)
    logger.info(f"{result=}")


@trap
def send_urmessage(destination:Iterable, subject:str, host:str='127.0.0.1', port:int=33333) -> bool:
    """
    Connect to the urmessage service's socket and send a short message
    consisting of just a subject line.

    destination -- a list of recipients' email addresses.
    subject -- subject line of the message
    host -- defaults to /this/ computer .. might be a bad choice, but you
        gotta have something.
    port -- 33333 is the default port for the urmessage service.
    """

    try:
        urmessage_service = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        urmessage_service.connect((host, port))
    except Exception as e:
        print(f"{e=}")
        sys.exit(os.EX_UNAVAILABLE)

    try:
        message = f"${destination}#{subject}$"
        urmessage_service.sendall(message.encode())
        return True

    except Exception as e:
        print(f"{e=}")
        return False

    finally:
        urmessage_service.close()


@trap
def urmessage_main(myargs:argparse.Namespace) -> int:
    """
    Establish the server socket, and open it to listen for
    incoming messages.
    """
    global logger

    not myargs.foreground and linuxutils.daemonize_me()

    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((myargs.host, myargs.port))
        server_socket.listen(3)
    except Exception as e:
        logger.error(f"Cannot open listen socket at {myargs.host}:{myargs.port}. Exception {e=}")
        sys.exit(os.EX_UNAVAILABLE)

    try:
        while True:
            client_socket, addr = server_socket.accept()
            # After accepting a connection, kick off a thread to handle
            # the connection, and go back to accepting connections.
            thread = threading.Thread(target=handle_message, args=(client_socket, addr))
            thread.start()

    finally:
        server_socket.close()

    return os.EX_OK


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(prog="urmessage", 
        description="What urmessage does, urmessage does best.")

    parser.add_argument('--config', type=str, default=f"{os.path.basename(__file__)[:-3]}.toml")
    parser.add_argument('-f', '--foreground', action='store_true',
        help="Run in the foreground rather than becoming a daemon.")
    parser.add_argument('--host', type=str, default='0.0.0.0')
    parser.add_argument('-L', '--loglevel', type=int, default=logging.INFO,
        choices=range(50,0,-10))
    parser.add_argument('-p', '--port', type=int, default=33333)
    parser.add_argument('-z', '--zap', action='store_true', 
        help='Remove old logfile.')

    myargs = parser.parse_args()

    ###
    # Send all relevant signals to the handler.
    ###
    for sig in [ signal.SIGINT, signal.SIGQUIT, signal.SIGTERM, signal.SIGHUP,
                        signal.SIGUSR1, signal.SIGUSR2 ]:
        signal.signal(sig, handler)

    ###
    # Load the configuration info.
    ###
    with open(myargs.config, 'rb') as f:
        myconfig = SloppyTree(tomllib.load(f)) # None

    ###
    # Open the logfile, removing the old one if asked.
    ###
    logfile  = f"{os.path.basename(__file__)[:-3]}.log" 
    if myargs.zap: 
        try:
            os.unlink(logfile)
        except:
            pass
    logger = URLogger(logfile=logfile, level=myargs.loglevel)
    logger.info('+++ BEGIN +++')

    ###
    # Check the lockfile to see if we are already running.
    ###
    lockfile = f"{os.path.basename(__file__)[:-3]}.lock" 
    if not fileutils.get_lockfile(lockfile):
        print(f"Cannot get {lockfile=}. This program is already running.")
        sys.exit(os.EX_UNAVAILABLE)
    else:
        print("Lock established.")
    
    try:
        sys.exit(globals()[f"{os.path.basename(__file__)[:-3]}_main"](myargs))

    except Exception as e:
        print(f"Escaped or re-raised exception: {e}")
    
    finally:
        fileutils.release_lockfile(lockfile)
    

