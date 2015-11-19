"""Script that checks for primary key violations on the input table
"""

import argparse
import pandas.io.sql as pdsql
from dataduct.data_access import redshift_connection
from dataduct.database import SqlScript
from dataduct.database import Table
from dataduct.qa import PrimaryKeyCheck


def primary_key_check():
    parser = argparse.ArgumentParser()

    parser.add_argument('--table', dest='table', required=True)
    parser.add_argument('--sns_topic_arn', dest='sns_topic_arn', default=None)
    parser.add_argument('--test_name', dest='test_name',
                        default="Check Primary Key")
    parser.add_argument('--log_to_s3', action='store_true', default=False)
    parser.add_argument('--path_suffix', dest='path_suffix', default=None)

    args = parser.parse_args()

    connection = redshift_connection()
    table = Table(SqlScript(args.table))
    result = pdsql.read_sql(table.select_duplicates_script().sql(), connection)
    check = PrimaryKeyCheck(len(result), name=args.test_name,
                            sns_topic_arn=args.sns_topic_arn)
    check.publish(args.log_to_s3, table=table.full_name,
                  path_suffix=args.path_suffix)
    connection.close()
