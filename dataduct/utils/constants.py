"""Constants shared across dataduct
"""
import os

# Constants
ZERO = 0
ONE = 1
NONE = None
EMPTY_STR = ''
NULL_STR = 'NULL'
DEFAULT_DELAY = '10 Minutes'
DEFAULT_TIMEOUT = '6 Hours'

# ETL Constants
EMR_CLUSTER_STR = 'emr'
EC2_RESOURCE_STR = 'ec2'
M1_LARGE = 'm1.large'

LOG_STR = 'logs'
DATA_STR = 'data'
SRC_STR = 'src'

# Step paths
SCRIPTS_DIRECTORY = 'scripts'
SCRIPT_RUNNER_PATH = os.path.join(SCRIPTS_DIRECTORY, 'script_runner.py')
DEPENDENCY_SCRIPT_PATH = os.path.join(SCRIPTS_DIRECTORY,
                                      'pipeline_dependency_check.py')
