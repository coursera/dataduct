"""
ETL step wrapper for QA step can be executed on Ec2 / EMR
"""
from .transform import TransformStep
from ..config import Config

config = Config()
SNS_TOPIC_ARN_WARNING = config.etl['SNS_TOPIC_ARN_WARNING']


class QATransformStep(TransformStep):
    """QATransform Step class that helps run scripts on resouces for QA checks
    """

    def __init__(self,
                 id,
                 pipeline_name,
                 script_arguments=None,
                 sns_topic_arn=SNS_TOPIC_ARN_WARNING,
                 **kwargs):
        """Constructor for the QATransformStep class

        Args:
            sns_arn(str): sns topic arn for QA steps
            script_arguments(list of str): list of arguments to the script
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if script_arguments is None:
            script_arguments = list()

        script_arguments.extend(
            [
                "--sns_topic_arn=%s" % sns_topic_arn,
                "--test_name=%s" % (pipeline_name + "." + id)
            ]
        )

        super(QATransformStep, self).__init__(
            id=id,
            script_arguments=script_arguments,
            **kwargs)
