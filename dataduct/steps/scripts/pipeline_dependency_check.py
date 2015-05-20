#!/usr/bin/env python
"""
Allows pipeline to have dependencies with other pipelines

Expected behaviour of dependency step:

1) If pipeline X does not depend on anything (dependency list is empty ""),
   then the transform step should exit safely (sys.exit)

2) Assume pipeline X depends on Y. If Y does not exist, then throw an
   exception saying "Pipeline Y not found".

3) Assume pipeline X depends on Y. If pipeline Y just sleeps for 10 minutes,
   then pipeline X should not finish until after Y finishes in 10 minutes.

4) Assume pipeline X depends on Y. Pipeline Y exists but no instances of Y ran
   today. Pipeline X should throw an exception saying "Y does not exist today".

5) Assume pipeline X depends on Y. Pipeline Y was "CANCELED"/"CANCELLED" today.
   Pipeline X should throw exception saying "Bad status"

6) Assume pipeline X depends on Y. Pipeline Y was "TIMEDOUT" today. Pipeline X
   should throw exception saying "Bad status"

7) Assume pipeline X depends on Y. Pipeline Y was "FAILED" today. Pipeline X
   should throw exception saying "Bad status"

8) Assume pipeline X depends on Y. Pipeline Y was "CASCADE_FAILED" today.
   Pipeline X should throw exception saying "Bad status"
"""

import argparse
import sys
import time
from datetime import datetime

from boto.sns import SNSConnection
from dataduct.pipeline.utils import list_pipelines
from dataduct.pipeline.utils import list_pipeline_instances


# Docs and API spelling of "CANCELED" don't match
FAILED_STATUSES = set(['CANCELED', 'CANCELLED', 'TIMEDOUT', 'FAILED',
                       'CASCADE_FAILED'])

# Pipeline attributes
STATUS = '@status'
START_TIME = '@scheduledStartTime'
FINISHED = 'FINISHED'


def check_dependencies_ready(dependencies, start_date, return_pipelines=False):
    """Checks if every dependent pipeline has completed

    Args:
        dependencies(list of str): list of pipeline name that it depends on
        start_date(str): string representing the start date of the pipeline
        return_pipelines(bool): return the failed/unfinished pipelines if true
    """

    print 'Checking dependency at ', str(datetime.now())

    dependency_ready = True

    # Convert date string to datetime object
    start_date = datetime.strptime(start_date, '%Y-%m-%d')

    for pipeline in dependencies:
        # Get instances of each pipeline
        instances = list_pipeline_instances(pipeline)
        failures = []
        unfinished = []

        # Collect all pipeline instances that are scheduled for today
        instances_today = []
        for instance in instances:
            date = datetime.strptime(instance[START_TIME], '%Y-%m-%dT%H:%M:%S')
            if date.date() == start_date.date():
                instances_today.append(instance)

        # Dependency pipeline has not started from today
        if not instances_today:
            dependency_ready = False

        for instance in instances_today:
            # One of the dependency failed/cancelled
            if instance[STATUS] in FAILED_STATUSES:
                if not return_pipelines:
                    raise Exception(
                        'Pipeline %s has bad status: %s'
                        % (pipeline, instance[STATUS])
                    )
                else:
                    failures.append(pipeline)
            # Dependency is still running
            elif instance[STATUS] != FINISHED:
                if return_pipelines:
                    unfinished.append(pipeline)
                dependency_ready = False

    # All dependencies are done
    if return_pipelines:
        return dependency_ready, failures, unfinished

    return dependency_ready


def main():
    """
    Main Function
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--dependencies', type=str, nargs='+', default=None)
    parser.add_argument('--refresh_rate', dest='refresh_rate', default='900')
    parser.add_argument('--start_date', dest='start_date')
    parser.add_argument('--ignore_dependencies', dest='ignore_dependencies', default='0')

    args = parser.parse_args()

    # Exit if there are no dependencies
    if not args.dependencies:
        sys.exit()

    # Create mapping from pipeline name to id
    pipeline_name_to_id = dict(
        (pipeline['name'], pipeline['id']) for pipeline in list_pipelines()
    )

    # Remove whitespace from dependency list
    dependencies = map(str.strip, args.dependencies)

    # Check if all dependencies are valid pipelines
    for dependency in dependencies:
        if dependency not in pipeline_name_to_id:
            raise Exception('Pipeline not found: %s.' % dependency)

    # Map from pipeline object to pipeline ID
    dependencies = [pipeline_name_to_id[dependency]
                    for dependency in dependencies]

    print 'Start checking for dependencies'
    start_time = datetime.now()

    dependencies_ready, failures, unfinished = check_dependencies_ready(dependencies,
                                                                        args.start_date,
                                                                        return_pipelines=True)


    # if ignoring dependencies, send message through SNS
    if not dependencies_ready and float(ignore_dependencies):
        sns_topic_arn = config.sns.get('AMAZON_RESOURCE_NAME', '')
        if sns_topic_arn:
            message = 'Failed dependencies: %s.' % ', '.join(failures)
            message += '\n Unfinished dependencies: %s.' % ', '.join(unfinished)
            subject = 'Dependency error'
            SNSConnection().publish(sns_topic_arn, message, subject)
        else:
            raise Exception('ARN for SNS topic not specified in config')

    # Loop until all dependent pipelines have finished
    while not dependencies_ready:
            print 'checking'
            time.sleep(float(args.refresh_rate))
            dependencies_ready = check_dependencies_ready(dependencies, args.start_date):

    print 'Finished checking for dependencies. Total time spent: ',
    print (datetime.now() - start_time).total_seconds(), ' seconds'


if __name__ == '__main__':
    main()
