"""
Pipeline object class for default metadata
"""

from .pipeline_object import PipelineObject
from ..config import Config

config = Config()
ROLE = config.etl['ROLE']
RESOURCE_ROLE = config.etl['RESOURCE_ROLE']
MAX_ACTIVE_INSTANCES = config.etl.get('MAX_ACTIVE_INSTANCES', 1)


class DefaultObject(PipelineObject):
    """Default object added to all pipelines
    """

    def __init__(self, id, pipeline_log_uri, sns=None, scheduleType='cron',
                 failureAndRerunMode='CASCADE', **kwargs):
        """Constructor for the DefaultObject class

        Args:
            id(str): must be 'Default' for this class
            sns(int): notify on failure
            scheduleType(str): frequency type for the pipeline
            failureAndRerunMode(str): aws input argument for failure mode
            **kwargs(optional): Keyword arguments directly passed to base class

        Note:
            id must be Default for this object
        """

        super(DefaultObject, self).__init__(
            id='Default', # This should always have the default id
            scheduleType=scheduleType,
            failureAndRerunMode=failureAndRerunMode,
            role=ROLE,
            resourceRole=RESOURCE_ROLE,
            maxActiveInstances=MAX_ACTIVE_INSTANCES,
            pipelineLogUri=pipeline_log_uri,
            onFail=sns
        )
