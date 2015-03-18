#!/usr/bin/env python
"""Runner for the upsert SQL step
"""
import argparse
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.database import SqlStatement
from dataduct.database import Table


def main():
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

    table = Table(SqlStatement(args.table_definition))
    connection = redshift_connection()
    # Enable autocommit for non transactional sql execution
    if args.non_transactional:
        connection.autocommit = True

    table_not_exists = pdsql.read_sql(table.check_not_exists_script().sql(),
                                      connection).loc[0][0]

    cursor = connection.cursor()
    # Create table in redshift, this is safe due to the if exists condition
    if table_not_exists:
        cursor.execute(table.create_script().sql())

    # Load data into redshift with upsert query
    # If there are sql_arguments, place them along with the query
    # Otherwise, don't include them to avoid having to use %% everytime
    if len(sql_arguments) > 1:
        print cursor.mogrify(args.sql, tuple(sql_arguments))
        cursor.execute(args.sql, tuple(sql_arguments))
    else:
        print args.sql
        cursor.execute(args.sql)
    cursor.execute('COMMIT')

    # Analyze the table
    if args.analyze:
        cursor.execute(table.analyze_script().sql())

    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
