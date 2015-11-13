"""Script that compares the number of rows in the source select script with the
number of rows in the destination table
"""

import argparse
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.data_access import rds_connection
from dataduct.qa import CountCheck


def _get_source_data(sql, hostname):
    """Gets the DataFrame containing all the rows of the table
    The DataFrame will be indexed by the table's primary key(s)

    Args:
        sql(str): The table definition representing the table to query
        connection(Connection): A connection to the database

    Returns:
        DataFrame: The rows of the table
    """
    connection = rds_connection(hostname)
    data = pdsql.read_sql(sql, connection)
    connection.close()
    return data.iloc[0][0]


def _get_destination_data(sql):
    """Gets the DataFrame containing all the rows of the table
    The DataFrame will be indexed by the table's primary key(s)

    Args:
        sql(str): The table definition representing the table to query
        connection(Connection): A connection to the database

    Returns:
        DataFrame: The rows of the table
    """
    connection = redshift_connection()
    data = pdsql.read_sql(sql, connection)
    connection.close()
    # All columns apart from last are PK columns
    return data.iloc[0][0]


def count_check():
    """Args (taken in through argparse):
        source_sql: SQL script of the source data
        destination_sql: SQL script of the destination data
    """
    parser = argparse.ArgumentParser()

    parser.add_argument('--source_sql', dest='source_sql', required=True)
    parser.add_argument('--source_host', dest='source_host', required=True)
    parser.add_argument('--destination_sql', dest='destination_sql',
                        required=True)
    parser.add_argument('--tolerance', type=float, dest='tolerance',
                        default=1.0)
    parser.add_argument('--sns_topic_arn', dest='sns_topic_arn', default=None)
    parser.add_argument('--test_name', dest='test_name',
                        default='Check Count')
    parser.add_argument('--log_to_s3', action='store_true', default=False)
    parser.add_argument('--path_suffix', dest='path_suffix', default=None)

    args = parser.parse_args()

    source_count = _get_source_data(args.source_sql, args.source_host)
    destination_count = _get_destination_data(args.destination_sql)

    check = CountCheck(source_count, destination_count,
                       name=args.test_name,
                       sns_topic_arn=args.sns_topic_arn,
                       tolerance=args.tolerance)

    check.publish(args.log_to_s3, dest_sql=args.destination_sql,
                  path_suffix=args.path_suffix)
