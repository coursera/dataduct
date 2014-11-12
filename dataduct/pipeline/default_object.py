"""
Pipeline object class for default metadata
"""

from .pipeline_object import PipelineObject
from ..config import Config

config = Config()
DEFAULT_ROLE = config.ec2['DEFAULT_ROLE']
DEFAULT_RESOURCE_ROLE = config.ec2['DEFAULT_RESOURCE_ROLE']


class DefaultObject(PipelineObject):
    """Default object added to all pipelines
    """

    def __init__(self,
                 id='Default',
                 sns=None,
                 scheduleType='cron',
                 failureAndRerunMode='CASCADE',
                 **kwargs):
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
            id=id,
            scheduleType=scheduleType,
            failureAndRerunMode=failureAndRerunMode,
            role=DEFAULT_ROLE,
            resourceRole=DEFAULT_RESOURCE_ROLE,
            onFail=sns
        )
