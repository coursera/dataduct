#!/usr/bin/env python

"""Script that checks if the rows of the destination table has been populated
with the correct values
"""

import argparse
import collections
import re
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.data_access import rds_connection
from dataduct.database import SqlScript
from dataduct.qa import ColumnCheck


def _get_source_data(sql, hostname, sample_size):
    """Gets the DataFrame containing all the rows of the table
    The DataFrame will be indexed by the table's primary key(s)

    Args:
        sql(str): The table definition representing the table to query
        connection(Connection): A connection to the database

    Returns:
        DataFrame: The rows of the table
    """
    connection = rds_connection(hostname)
    query = re.sub(
        r'(?i)LIMIT_PLACEHOLDER',
        str(sample_size),
        sql,
    )

    data = pdsql.read_sql(query, connection)
    connection.close()
    # All columns apart from last are PK columns
    return data.set_index(list(data.columns[:-1]))


def _get_destination_data(sql, primary_keys):
    """Gets the DataFrame containing all the rows of the table
    The DataFrame will be indexed by the table's primary key(s)

    Args:
        sql(str): The table definition representing the table to query

    Returns:
        DataFrame: The rows of the table
    """
    connection = redshift_connection()

    # Make primary_keys always a list of tuples
    if isinstance(primary_keys[0], basestring):
        primary_keys = [(pk) for pk in primary_keys]

    # Check whether it is not iterable
    if not isinstance(primary_keys, collections.Iterable):
        primary_keys = [tuple([pk]) for pk in primary_keys]

    # Format primary key string
    primary_key_string = re.sub(
        r",\)",
        ")",
        str(tuple(primary_keys))
    )

    # If a key is Timestamp, the output string needs to be fixed.
    # e.g., from Timestamp('2014-06-09 05:13:11') to '2014-06-09 05:13:11'
    primary_key_string = re.sub(r"Timestamp\(([^,]*)[^)]*\)", r"\1",
                                primary_key_string)

    query = re.sub(
        r'(?i)PRIMARY_KEY_SET',
        primary_key_string,
        sql,
    )

    data = pdsql.read_sql(query, connection)
    connection.close()
    # All columns apart from last are PK columns
    return data.set_index(list(data.columns[:-1]))


def main():
    """Main function

    Args (taken in through argparse):
        source_sql: SQL script of the source data
        destination_sql: SQL script of the destination data
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--source_sql', dest='source_sql', required=True)
    parser.add_argument('--source_host', dest='source_host', required=True)
    parser.add_argument('--destination_sql', dest='destination_sql',
                        required=True)
    parser.add_argument('--sample_size', dest='sample_size', required=True)
    parser.add_argument('--tolerance', type=float, dest='tolerance',
                        default=0.1)
    parser.add_argument('--sns_topic_arn', dest='sns_topic_arn', default=None)
    parser.add_argument('--test_name', dest='test_name',
                        default='Check Column')

    args = parser.parse_args()

    # Open up a connection and read the source and destination tables
    source_data = _get_source_data(args.source_sql, args.source_host,
                                   args.sample_size)
    destination_data = _get_destination_data(args.destination_sql,
                                             list(source_data.index))

    check = ColumnCheck(source_data, destination_data,
                        name=args.test_name,
                        sns_topic_arn=args.sns_topic_arn,
                        tolerance=args.tolerance)
    check.publish()


if __name__ == '__main__':
    main()
