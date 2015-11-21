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
COMMAND_TEMPLATE = 'python -c "from {file} import {func}; {func}()" "$@"'

COUNT_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.count_check',
    func='count_check')

COLUMN_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.column_check',
    func='column_check')

LOAD_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.create_load_redshift',
    func='create_load_redshift_runner')

PK_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.primary_key_check',
    func='primary_key_check')

DEPENDENCY_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.dependency_check',
    func='dependency_check')

SCRIPT_RUNNER_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.runner', func='script_runner')

SQL_RUNNER_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.runner', func='sql_runner')
