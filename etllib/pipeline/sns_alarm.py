"""
Pipeline object class for sns
"""

from .pipeline_object import PipelineObject
from ..constants import DATA_PIPELINE_TOPIC_ARN


class SNSAlarm(PipelineObject):
    """Default object added to all pipelines
    """

    def __init__(self,
                 id,
                 pipeline_name=None,
                 failure_message=None,
                 **kwargs):
        """Constructor for the SNSAlarm class

        Args:
            id(str): id of the object
            pipeline_name(str): frequency type for the pipeline
            failure_message(str): Message used in SNS on pipeline failures,
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if not pipeline_name:
            pipeline_name = "None"

        if not failure_message:
            failure_message = '\n'.join([
                'Identifier: ' + pipeline_name,
                'Object: #{node.name}',
                'Object Scheduled Start Time: #{node.@scheduledStartTime}',
                'Error Message: #{node.errorMessage}',
                'Error Stack Trace: #{node.errorStackTrace}'
            ])

        super(SNSAlarm, self).__init__(
            id=id,
            type='SnsAlarm',
            topicArn=DATA_PIPELINE_TOPIC_ARN,
            subject='Data Pipeline Failure',
            message=failure_message,
            **kwargs
        )
