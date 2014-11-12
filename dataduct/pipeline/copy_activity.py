"""
Pipeline object class for CopyActivity
"""

from .activity import Activity
from .schedule import Schedule

from ..config import Config
from ..utils.exceptions import ETLInputError

config = Config()
DEFAULT_MAX_RETRIES = config.etl['DEFAULT_MAX_RETRIES']
RETRY_DELAY = config.etl['RETRY_DELAY']


class CopyActivity(Activity):
    """EC2 Resource class
    """

    def __init__(self,
                 id,
                 input_node,
                 output_node,
                 resource,
                 schedule,
                 max_retries=None,
                 depends_on=None,
                 **kwargs):
        """Constructor for the CopyActivity class

        Args:
            id(str): id of the object
            input_node(S3Node / list of S3Nodes): input nodes for the activity
            output_node(S3Node / list of S3Nodes): output nodes for activity
            resource(Ec2Resource / EmrResource): resource to run the activity on
            schedule(Schedule): schedule of the pipeline
            max_retries(int): number of retries for the activity
            depends_on(list of activities): dependendent pipelines steps
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        # Set default values
        if depends_on is None:
            depends_on = []
        if max_retries is None:
            max_retries = DEFAULT_MAX_RETRIES

        super(CopyActivity, self).__init__(
            id=id,
            retryDelay=RETRY_DELAY,
            type='CopyActivity',
            maximumRetries=max_retries,
            dependsOn=depends_on,
            input=input_node,
            output=output_node,
            runsOn=resource,
            schedule=schedule,
        )
