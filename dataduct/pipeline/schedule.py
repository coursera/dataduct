"""
Pipeline object class for the schedule
"""
from datetime import datetime
from datetime import timedelta
from pytimeparse import parse

from ..config import Config
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from .pipeline_object import PipelineObject

import logging
logger = logging.getLogger(__name__)

config = Config()
DAILY_LOAD_TIME = config.etl.get('DAILY_LOAD_TIME', const.ONE)


FEQUENCY_PERIOD_CONVERTION = {
    'weekly': ('1 week', None),
    '1-week': ('1 week', None),
    '2-weeks': ('2 weeks', None),
    'daily': ('1 day', None),
    '1-day': ('1 day', None),
    '2-days': ('2 days', None),
    '3-days': ('3 days', None),
    '4-days': ('4 days', None),
    '5-days': ('5 days', None),
    '6-days': ('6 days', None),
    'hourly': ('1 hour', None),
    '1-hour': ('1 hour', None),
    '2-hours': ('2 hours', None),
    '3-hours': ('3 hours', None),
    '4-hours': ('4 hours', None),
    '6-hours': ('6 hours', None),
    '8-hours': ('8 hours', None),
    '12-hours': ('12 hours', None),
    'one-time': ('15 minutes', 1),
}


class Schedule(PipelineObject):
    """Schedule object added to all pipelines
    """

    def __init__(self,
                 id,
                 frequency='one-time',
                 time_delta=None,
                 load_hour=None,
                 load_minutes=None,
                 **kwargs):
        """Constructor for the Schedule class

        Args:
            id(str): id of the Schedule object
            frequency(enum): rate at which pipeline should be run \
                can be daily, hourly and one-time
            time_delta(timedelta): Additional offset provided to the schedule
            load_hour(int): Hour at which the pipeline should start
            load_minutes(int): Minutes at which the pipeline should be run
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        current_time = datetime.utcnow()
        # Set the defaults for load hour and minutes
        if load_minutes is None:
            load_minutes = 0

        if load_hour is None:
            load_hour = DAILY_LOAD_TIME

        if time_delta is None:
            time_delta = timedelta(seconds=0)
        elif isinstance(time_delta, int):
            time_delta = timedelta(days=time_delta)
        elif not isinstance(time_delta, timedelta):
            raise ETLInputError(
                'Time_delta must be an instance of timedelta or int')

        if frequency in FEQUENCY_PERIOD_CONVERTION:
            period, occurrences = FEQUENCY_PERIOD_CONVERTION[frequency]
        else:
            raise ETLInputError(
                'Frequency %s not supported' % frequency)

        # Calculate the start time of the pipeline
        start_time = current_time.replace(minute=load_minutes)
        start_time = start_time.replace(hour=load_hour, second=0)

        if current_time.hour < load_hour:
            if frequency == 'one-time':
                time_delta -= timedelta(days=1)
            else:
                time_delta -= timedelta(seconds=parse(period))

        start_time += time_delta
        start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%S')
        logger.info('Pipeline scheduled to start at %s' % start_time_str)

        super(Schedule, self).__init__(
            id=id,
            type='Schedule',
            startDateTime=start_time_str,
            period=period,
            occurrences=occurrences
        )
