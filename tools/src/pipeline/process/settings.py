#!/usr/bin/python
import time
import getpass
from datetime import datetime
import glob
import multiprocessing
import os
import sys
import traceback
import socket
import redis
import json
from subprocess import Popen, PIPE
from couchdbkit import *

# this is user set
BASE_DIR = '/opt/NOW'

HAS_NODES = False #false is for single computer environment
NODE_BASE_NAME = socket.gethostname()
NODE_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

COUCHDB_HOST = "http://localhost:5984/"
COUCHDB_DB = "pipeline"

UG_NCT = 4
UG_NT = 15

# below here should be dynamic
DATE_FMT = "%Y%m%d %H:%M:%S"
DATE_FMT_WO = "%H:%M:%S"
DATE_FMT_FILE = "%Y%m%d_%H%M%S"

MAX_CPU = multiprocessing.cpu_count()

RET_OK = 0
RET_NO_WORK = -1
RET_ERROR = 1

REF_DIR = BASE_DIR + "/ref"
DATA_DIR = BASE_DIR + "/data"
STAT_DIR = BASE_DIR + "/stats"
LOG_DIR = BASE_DIR + "/log"
HOSTNAME = socket.gethostname()
WORK_DIR = BASE_DIR + "/data/" + HOSTNAME
SCRIPT_CLI = BASE_DIR + '/tools/src/pipeline/process-cli.py'
USERNAME = os.getlogin()
PASS = ""

server_process = []
node_processes = []

r = None

