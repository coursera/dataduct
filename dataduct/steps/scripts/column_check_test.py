#!/usr/bin/env python

"""Script that checks if the rows of the destination table has been populated
with the correct values
"""

import argparse
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.database import SqlScript
from dataduct.database import Table
from dataduct.qa import ColumnCheck


def _get_data(sql, connection):
    """Gets the DataFrame containing all the rows of the table
    The DataFrame will be indexed by the table's primary key(s)

    Args:
        sql(str): The table definition representing the table to query
        connection(Connection): A connection to the database

    Returns:
        DataFrame: The rows of the table
    """
    table = Table(SqlScript(sql))
    return pdsql.read_sql(table.select_script().sql(),
                          connection,
                          index_col=table.primary_key_names)


def main():
    """Main function

    Args (taken in through argparse):
        source_table: SQL script of the source table
        destination_table: SQL script of the destination table
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--source_table', dest='source_table', required=True)
    parser.add_argument('--destination_table', dest='destination_table',
                        required=True)
    parser.add_argument('--sns_topic_arn', dest='sns_topic_arn', default=None)
    parser.add_argument('--test_name', dest='test_name',
                        default='Check Column')

    args = parser.parse_args()

    # Open up a connection and read the source and destination tables
    connection = redshift_connection()
    source_data = _get_data(args.source_table, connection)
    destination_data = _get_data(args.destination_table, connection)

    check = ColumnCheck(source_data, destination_data, name=args.test_name,
                        sns_topic_arn=args.sns_topic_arn)
    check.publish()
    connection.close()


if __name__ == '__main__':
    main()
