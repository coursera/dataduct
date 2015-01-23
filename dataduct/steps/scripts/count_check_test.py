#!/usr/bin/env python

"""Script that compares the number of rows in the source select script with the
number of rows in the destination table
"""

import argparse
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.database import SqlScript
from dataduct.database import Table
from dataduct.qa import CountCheck


def main():
    """Main function

    Args (taken in through argparse):
        source_script: SQL script used in the pipeline
        destination_table: SQL script of the destination table
    """

    parser = argparse.ArgumentParser()

    parser.add_argument('--source_script', dest='source_script',
                        required=True)
    parser.add_argument('--destination_table', dest='destination_table',
                        required=True)
    parser.add_argument('--sns_topic_arn', dest='sns_topic_arn', default=None)
    parser.add_argument('--test_name', dest='test_name',
                        default='Check Count')

    args = parser.parse_args()

    connection = redshift_connection()
    source_count = len(pdsql.read_sql(args.source_script, connection))
    destination_table = Table(SqlScript(args.destination_table))
    destination_count = len(pdsql.read_sql(
        destination_table.select_script().sql(),
        connection))

    check = CountCheck(source_count, destination_count, name=args.test_name,
                       sns_topic_arn=args.sns_topic_arn)
    check.publish()
    connection.close()


if __name__ == '__main__':
    main()
