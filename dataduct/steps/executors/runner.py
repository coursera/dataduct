"""
This script initiates the different calls needed when running
a transform step with the script_directory argument
"""

# imports
import argparse
import os
import pandas.io.sql as pdsql
import subprocess

from dataduct.data_access import redshift_connection
from dataduct.database import SqlStatement
from dataduct.database import Table
from dataduct.s3 import S3File
from dataduct.s3 import S3Path


def run_command(arguments):
    """
    Args:
        arguments(list of str): Arguments to be executed as a command.
        Arguments are passed as if calling subprocess.call() directly
    """
    return subprocess.call(arguments)


def script_runner():
    """
    Parses the command line arguments and runs the suitable functions
    """
    parser = argparse.ArgumentParser()
    # Environment variable for the source directory
    parser.add_argument('--INPUT_SRC_ENV_VAR', dest='input_src_env_var')

    # Argument for script name
    parser.add_argument('--SCRIPT_NAME', dest='script_name')
    args, ext_script_args = parser.parse_known_args()

    # Check if the source directory exists
    input_src_dir = os.getenv(args.input_src_env_var)
    if not os.path.exists(input_src_dir):
        raise Exception(input_src_dir + " does not exist")

    run_command(['ls', '-l', input_src_dir])
    run_command(['chmod', '-R', '+x', input_src_dir])
    run_command(['ls', '-l', input_src_dir])

    input_file = os.path.join(input_src_dir, args.script_name)
    result = run_command([input_file] + ext_script_args)
    if result != 0:
        raise Exception("Script failed.")


def sql_runner():
    """Main Function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_definition', dest='table_definition',
                        required=True)
    parser.add_argument('--sql', dest='sql', required=True)
    parser.add_argument('--analyze', action='store_true', default=False)
    parser.add_argument('--non_transactional', action='store_true',
                        default=False)

    args, sql_arguments = parser.parse_known_args()
    print args, sql_arguments

    sql_query = args.sql
    if sql_query.startswith('s3://'):
        sql_query = S3File(s3_path=S3Path(uri=args.sql)).text

    table = Table(SqlStatement(args.table_definition))
    connection = redshift_connection()
    # Enable autocommit for non transactional sql execution
    if args.non_transactional:
        connection.autocommit = True
    else:
        # connection by default sets autocommit to True, but for
        # the SQL runner, it should be False to put all SQLs into one
        # transaction.
        connection.autocommit = False

    table_not_exists = pdsql.read_sql(table.check_not_exists_script().sql(),
                                      connection).loc[0][0]

    cursor = connection.cursor()
    # Create table in redshift, this is safe due to the if exists condition
    if table_not_exists:
        cursor.execute(table.create_script().sql())

    # Load data into redshift with upsert query
    # If there are sql_arguments, place them along with the query
    # Otherwise, don't include them to avoid having to use %% everytime
    if len(sql_arguments) >= 1:
        print cursor.mogrify(sql_query, tuple(sql_arguments))
        cursor.execute(sql_query, tuple(sql_arguments))
    else:
        print sql_query
        cursor.execute(sql_query)
    cursor.execute('COMMIT')

    # Analyze the table
    if args.analyze:
        cursor.execute(table.analyze_script().sql())

    cursor.close()
    connection.close()
