"""Replacement for the load step to use the redshift COPY command instead
"""

import argparse
import pandas.io.sql as pdsql
import psycopg2.extras

from dataduct.config import get_aws_credentials
from dataduct.data_access import redshift_connection
from dataduct.database import SqlStatement
from dataduct.database import Table
from dataduct.utils.helpers import stringify_credentials
from sys import stderr


def load_redshift(table, input_paths, max_error=0,
                  replace_invalid_char=None, no_escape=False, gzip=False,
                  command_options=None):
    """Load redshift table with the data in the input s3 paths
    """
    table_name = table.full_name
    print 'Loading data into %s' % table_name

    # Credentials string
    aws_key, aws_secret, token = get_aws_credentials()
    creds = stringify_credentials(aws_key, aws_secret, token)

    delete_statement = 'TRUNCATE %s;' % table_name
    error_string = 'MAXERROR %d' % max_error if max_error > 0 else ''
    if replace_invalid_char is not None:
        invalid_char_str = "ACCEPTINVCHARS AS %s" % replace_invalid_char
    else:
        invalid_char_str = ''

    query = [delete_statement]

    template = (
        "COPY {table} FROM '{path}' WITH CREDENTIALS AS '{creds}' "
        "COMPUPDATE OFF STATUPDATE OFF {options};"
    )

    for input_path in input_paths:
        if not command_options:
            command_options = (
                "DELIMITER '\t' {escape} {gzip} NULL AS 'NULL' "
                "TRUNCATECOLUMNS {max_error} {invalid_char_str}"
            ).format(escape='ESCAPE' if not no_escape else '',
                     gzip='GZIP' if gzip else '',
                     max_error=error_string,
                     invalid_char_str=invalid_char_str)

        statement = template.format(table=table_name,
                                    path=input_path,
                                    creds=creds,
                                    options=command_options)
        query.append(statement)

    return ' '.join(query)


def create_error_retrieval_query(input_paths):
    condition = ("filename Like '%{input_path}%'".format(input_path=input_path)
                 for input_path in input_paths)
    conditions = " OR ".join(condition)
    query = ("SELECT * FROM stl_load_errors "
             "WHERE {conditions}").format(conditions=conditions)
    return query


def get_redshift_table_colunms(table, cursor):
    table_name = table.table_name
    schema_name = table.schema_name

    set_search_path_query = "SET search_path TO {schema}".format(
        schema=schema_name)
    cursor.execute(set_search_path_query)
    query = ("SELECT def.column FROM pg_table_def def "
             "WHERE schemaname = '{schema_name}' "
             "AND tablename = '{table_name}'").format(schema_name=schema_name,
                                                      table_name=table_name)
    cursor.execute(query)
    columns = sorted([row["column"] for row in cursor.fetchall()])
    return columns


def create_load_redshift_runner():
    parser = argparse.ArgumentParser()
    parser.add_argument('--table_definition', dest='table_definition',
                        required=True)
    parser.add_argument('--max_error', dest='max_error', default=0, type=int)
    parser.add_argument('--replace_invalid_char', dest='replace_invalid_char',
                        default=None)
    parser.add_argument('--no_escape', action='store_true', default=False)
    parser.add_argument('--gzip', action='store_true', default=False)
    parser.add_argument('--command_options', dest='command_options',
                        default=None)
    parser.add_argument('--s3_input_paths', dest='input_paths', nargs='+')
    parser.add_argument('--force_drop_table', dest='force_drop_table',
                        default=False)
    script_arguments = parser.parse_args()
    print script_arguments

    table = Table(SqlStatement(script_arguments.table_definition))
    connection = redshift_connection(
        cursor_factory=psycopg2.extras.RealDictCursor)
    table_not_exists = pdsql.read_sql(table.check_not_exists_script().sql(),
                                      connection).loc[0][0]

    cursor = connection.cursor()
    # Create table in redshift, this is safe due to the if exists condition
    if table_not_exists:
        cursor.execute(table.create_script().sql())
    else:
        columns = sorted(
            [column.column_name.lower() for column in table.columns()])
        redshift_table_columns = get_redshift_table_colunms(table, cursor)

        if columns != redshift_table_columns:
            error_string = (
                "Table schema mismatch: {table}\n"
                "Columns for new table: {columns}\n"
                "Columns for existing table: {redshift_table_columns}").format(
                    table=table.full_name,
                    columns=", ".join(columns),
                    redshift_table_columns=", ".join(redshift_table_columns))
            raise Exception(error_string)

    # Load data into redshift
    load_query = load_redshift(
        table, script_arguments.input_paths, script_arguments.max_error,
        script_arguments.replace_invalid_char, script_arguments.no_escape,
        script_arguments.gzip, script_arguments.command_options)
    try:
        cursor.execute(load_query)
        cursor.execute('COMMIT')
    except Exception as error:
        error_query = create_error_retrieval_query(
            script_arguments.input_paths)
        cursor.execute(error_query)
        separator = "-" * 50 + "\n"

        stderr.write(
            "Error while loading data into redshift \n\n{}".format(separator))

        for item in cursor.fetchall():
            for key in item:
                stderr.write("{}: {}\n".format(key, str(item[key]).strip()))
            stderr.write(separator)
        raise error

    cursor.close()
    connection.close()
