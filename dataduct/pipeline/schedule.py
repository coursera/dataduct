"""
Pipeline object class for the schedule
"""
from datetime import datetime
from datetime import timedelta

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
DAILY_LOAD_TIME = config.etl.get('DAILY_LOAD_TIME', const.ONE)


FEQUENCY_PERIOD_CONVERTION = {
    'daily': ('1 day', None),
    'hourly': ('1 hour', None),
    'one-time': ('15 minutes', 1),
}


class Schedule(PipelineObject):
    """Schedule object added to all pipelines
    """

    def __init__(self,
                 id,
                 frequency='one-time',
                 delay=None,
                 load_hour=None,
                 load_minutes=None,
                 **kwargs):
        """Constructor for the Schedule class

        Args:
            id(str): id of the Schedule object
            frequency(enum): rate at which pipeline should be run \
                can be daily, hourly and one-time
            delay(timedelta): Additional offset provided to the schedule
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

        if delay is None:
            delay = timedelta(0)
        elif isinstance(delay, int):
            delay = timedelta(days=delay)
        elif not isinstance(delay, timedelta):
            raise ETLInputError('Delay must be an instance of timedelta or int')

        if frequency in FEQUENCY_PERIOD_CONVERTION:
            period, occurrences = FEQUENCY_PERIOD_CONVERTION[frequency]
        else:
            raise ETLInputError(
                'Frequency for the pipeline must be daily, hourly and one-time')

        # Calculate the start time of the pipeline
        start_time = current_time.replace(minute=load_minutes)
        if frequency == 'daily':
            start_time = start_time.replace(hour=load_hour)

        if current_time.hour < load_hour:
            delay += timedelta(days=-1)

        start_time += delay

        super(Schedule, self).__init__(
            id=id,
            type='Schedule',
            startDateTime=start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            period=period,
            occurrences=occurrences
        )
