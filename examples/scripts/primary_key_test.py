#!/usr/bin/env python


import argparse
import pandas.io.sql as pdsql
from dataduct.qa import PrimaryKeyCheck
# from datapipeline.database.table import Table
# from datapipeline.qa.check import Check
# from datapipeline.qa.check import get_sns_alert_fn
# from datapipeline.qa.s3 import qa_check_export_fn
# from datapipeline.data_access.connections import redshift_connection


def query_redshift(production, query):
    """
    Input:
        - prod -- whether to reference the prod table
        - query -- a query that computes a count
    Output:
        - the value returned by the query
    """
    print "Running query", query
    return pdsql.read_sql(query, redshift_connection(production))


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
    result = query_redshift(
        args.production,
        table.select_duplicates_sql().raw_sql(),
    )

    check = PrimaryKeyCheck(
        len(result), args.test_name, get_sns_alert_fn(args.sns_topic))
    check.publish(qa_check_export_fn(
        args.production, args.pipeline_name, table=table.full_name))

    print "Passed test."
