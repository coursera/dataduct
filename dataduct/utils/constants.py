"""Constants shared across dataduct
"""

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

# Commands
ARGUMENT_PROPOGATOR = ' "$@"'
COUNT_CHECK_COMMAND = (
    'python -c "from dataduct.steps.executors.count_check import '
    'count_check; count_check()"') + ARGUMENT_PROPOGATOR

COLUMN_CHECK_COMMAND = (
    'python -c "from dataduct.steps.executors.column_check import '
    'column_check; column_check()"') + ARGUMENT_PROPOGATOR

LOAD_COMMAND = (
    'python -c "from dataduct.steps.executors.create_load_redshift import '
    'create_load_redshift_runner; create_load_redshift_runner()"') +\
    ARGUMENT_PROPOGATOR

PK_CHECK_COMMAND = (
    'python -c "from dataduct.steps.executors.primary_key_check import '
    'primary_key_check; primary_key_check()"') + ARGUMENT_PROPOGATOR

DEPENDENCY_COMMAND = (
    'python -c "from dataduct.steps.executors.dependency_check import '
    'dependency_check; dependency_check()"') + ARGUMENT_PROPOGATOR

SCRIPT_RUNNER_COMMAND = (
    'python -c "from dataduct.steps.executors.runner import '
    'script_runner; script_runner()"') + ARGUMENT_PROPOGATOR

SQL_RUNNER_COMMAND = (
    'python -c "from dataduct.steps.executors.runner import '
    'sql_runner; sql_runner()"') + ARGUMENT_PROPOGATOR
