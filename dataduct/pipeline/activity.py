"""
Base class for data pipeline instance
"""

from .pipeline_object import PipelineObject


class Activity(PipelineObject):
    """Base class for pipeline activities
    """

    def __init__(self, dependsOn, maximumRetries, **kwargs):
        """Constructor for the activity class

        Args:
            dependsOn(list): list of dependent activities
            maximumRetries(int): maximum number of retries
            **kwargs(optional): Keyword arguments directly passed to base class

        Note:
            dependsOn and maximum retries are required fields for any activity
        """
        super(Activity, self).__init__(
            dependsOn=dependsOn,
            maximumRetries=maximumRetries,
            **kwargs
        )

    @property
    def input(self):
        """Get the input node for the activity

        Returns:
            result: Input node for this activity
        """
        return self['input']

    @property
    def output(self):
        """Get the output node for the activity

        Returns:
            result: output node for this activity
        """
        return self['output']

    @property
    def depends_on(self):
        """Get the dependent activities for the activity

        Returns:
            result: dependent activities for this activity
        """
        return self['dependsOn']

    @property
    def maximum_retries(self):
        """Get the maximum retries for the activity

        Returns:
            result: maximum retries for this activity
        """
        return self['maximumRetries']
