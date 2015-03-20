"""
Shared utility functions
"""
from boto.datapipeline import regions
from boto.datapipeline.layer1 import DataPipelineConnection
from time import sleep
import dateutil.parser

from dataduct.config import Config

config = Config()
REGION = config.etl.get('REGION', None)

DP_ACTUAL_END_TIME = '@actualEndTime'
DP_ATTEMPT_COUNT_KEY = '@attemptCount'
DP_INSTANCE_ID_KEY = 'id'
DP_INSTANCE_STATUS_KEY = '@status'


def _update_sleep_time(last_time=None):
    """Expotentially decay sleep times between calls incase of failures

    Note:
        Start time for sleep is 5 and the max is 60

    Args:
        last_time(int): time used in the last iteration

    Returns:
        next_time(int): time to sleep in the next iteration of the code

    """
    start_sleep_time = 5
    max_sleep_time = 60

    if last_time is None:
        return start_sleep_time

    return min(last_time * 2, max_sleep_time)


def get_response_from_boto(fn, *args, **kwargs):
    """Expotentially decay sleep times between calls incase of failures

    Note:
        If there is a rate limit error, sleep until the error goes away

    Args:
        func(function): Function to call
        args(optional): arguments
        kwargs(optional): keyword arguments

    Returns:
        response(json): request response.

    Input:
        func(function): Function to call
        args(optional): arguments
        kwargs(optional): keyword arguments
    """

    response = None
    sleep_time = None
    while response is None:
        try:
            response = fn(*args, **kwargs)
        except Exception, error:
            if error.error_code != 'ThrottlingException':
                raise
            else:
                sleep_time = _update_sleep_time(sleep_time)
                print "Rate limit exceeded. Sleeping %d seconds." % sleep_time
                sleep(sleep_time)
    return response


def get_list_from_boto(func, response_key, *args, **kwargs):
    """Get a paginated list from boto

    Args:
        func(function): Function to call
        response_key(str): Key which points to a list
        *args(optional): arguments
        **kwargs(optional): keyword arguments

    Returns:
        results(list): Aggregated list of items indicated by the response key
    """
    results = []
    has_more_results, marker = True, None

    while has_more_results:

        kwargs['marker'] = marker
        response = get_response_from_boto(func, *args, **kwargs)
        has_more_results = response['hasMoreResults']

        if has_more_results:
            marker = response['marker']
        results.extend(response[response_key])

    return results


def list_pipeline_instances(pipeline_id, conn=None, increment=25):
    """List details of all the pipeline instances

    Args:
        pipeline_id(str): id of the pipeline
        conn(DataPipelineConnection): boto connection to datapipeline
        increment(int): rate of increments in API calls

    Returns:
        instances(list): list of pipeline instances
    """
    if conn is None:
        conn = get_datapipeline_connection()

    # Get all instances
    instance_ids = sorted(get_list_from_boto(conn.query_objects,
                                             'ids',
                                             pipeline_id,
                                             'INSTANCE'))

    # Collect all instance details
    instances = []
    for start in range(0, len(instance_ids), increment):

        # Describe objects in batches as API is rate limited
        response = get_response_from_boto(
            conn.describe_objects,
            instance_ids[start:start + increment],
            pipeline_id,
        )

        for pipeline_object in response['pipelineObjects']:
            pipeline_dict = dict(
                (
                    sub_dict['key'],
                    sub_dict.get('stringValue', sub_dict.get('refValue', None))
                )
                for sub_dict in pipeline_object['fields']
            )
            pipeline_dict['id'] = pipeline_object['id']

            # Append to all instances
            instances.append(pipeline_dict)

    return instances


def get_datapipeline_connection():
    """Get boto connection of AWS data pipeline

    Returns:
        DataPipelineConnection: boto connection
    """
    region = next((x for x in regions() if x.name == str(REGION).lower()), None)
    conn = DataPipelineConnection(region=region)
    return conn


def list_pipelines(conn=None):
    """Fetch a list of all pipelines with boto

    Args:
        conn(DataPipelineConnection): boto connection to datapipeline

    Returns:
        pipelines(list): list of pipelines fetched with boto
    """
    if conn is None:
        conn = get_datapipeline_connection()

    return get_list_from_boto(
        conn.list_pipelines,
        'pipelineIdList',
    )


def date_string(date):
    """Normalizes a date string to YYYY-mm-dd HH:MM:SS
    """
    if date is None:
        return 'NULL'
    return str(dateutil.parser.parse(date))


def list_formatted_instance_details(pipeline):
    """List of instance rows formatted to match
    """
    etl_runs = pipeline.instance_details()
    entries = []
    for etl_run_dt in sorted(etl_runs.keys()):

        # Look through instances
        for instance in sorted(
                etl_runs[etl_run_dt],
                key=lambda x: x.get(DP_ACTUAL_END_TIME, None)):
            entries.append(
                [
                    instance[DP_INSTANCE_ID_KEY],
                    pipeline.id,
                    date_string(etl_run_dt),
                    date_string(instance.get(DP_ACTUAL_END_TIME)),
                    instance[DP_INSTANCE_STATUS_KEY],
                    instance.get(DP_ATTEMPT_COUNT_KEY, 'NULL'),
                ]
            )
    return entries
