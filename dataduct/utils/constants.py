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
QA_STR = 'qa'

# Step paths
SCRIPTS_DIRECTORY = 'scripts'
SCRIPT_RUNNER_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'script_runner.py')
DEPENDENCY_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'pipeline_dependency_check.py')
PK_CHECK_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'primary_key_test.py')
COUNT_CHECK_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'count_check_test.py')
COLUMN_CHECK_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'column_check_test.py')
CREATE_LOAD_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'create_load_redshift_runner.py')
SQL_RUNNER_SCRIPT_PATH = os.path.join(
    SCRIPTS_DIRECTORY, 'sql_runner.py')

#pg_table_def search paths
MANAGED_SCHEMAS = ["prod", "bi", "scratch", "staging", "dev", "devbi", "devstaging", "hist",
    "edw_test", "devhist", "devmaster", "biv", "prodv"]
