"""
Pipeline object class for the schedule
"""
from datetime import datetime
from datetime import timedelta
from pytimeparse import parse

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
DAILY_LOAD_TIME = config.etl.get('DAILY_LOAD_TIME', const.ONE)


FEQUENCY_PERIOD_CONVERTION = {
    'weekly': ('1 week', None),
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
            raise ETLInputError('time_delta must be an instance of timedelta or int')

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
            if frequency == 'one-time':
                time_delta -= timedelta(days=1)
            else:
                time_delta -= timedelta(seconds=parse(period))

        start_time += time_delta

        super(Schedule, self).__init__(
            id=id,
            type='Schedule',
            startDateTime=start_time.strftime('%Y-%m-%dT%H:%M:%S'),
            period=period,
            occurrences=occurrences
        )
