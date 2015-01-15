"""Script that checks for primary key violations on the input table
"""
#!/usr/bin/env python

import argparse
import pandas.io.sql as pdsql
from dataduct.qa import PrimaryKeyCheck
from dataduct.data_access.connection import redshift_connection


def query_redshift(production, query):
    """
    Input:
        - prod -- whether to reference the prod table
        - query -- a query that computes a count
    Output:
        - the value returned by the query
    """
    print "Running query", query
    return pdsql.read_sql(query, redshift_connection())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--table', dest='table', required=True)
    parser.add_argument('--production', dest='production', action='store_true')
    parser.add_argument('--pipeline_name', dest='pipeline_name', required=True)

    parser.add_argument(
        '--sns_topic', dest='sns_topic', default=None)
    parser.add_argument(
        '--test_name', dest='test_name', default="Check Maestro Column")

    args = parser.parse_args()
    print "Got args for check primary key", args

    table = Table(script=args.table)
    result = pdsql.read_sql(
        table.select_duplicates_sql().raw_sql(), redshift_connection())

    check = PrimaryKeyCheck(
        len(result), args.test_name, get_sns_alert_fn(args.sns_topic))
    check.publish(qa_check_export_fn(
        args.production, args.pipeline_name, table=table.full_name))

    print "Passed test."
