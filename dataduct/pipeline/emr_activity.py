"""
Pipeline object class for EmrActivity
"""

from .activity import Activity
from ..config import Config
from .schedule import Schedule
from ..utils.exceptions import ETLInputError

config = Config()
MAX_RETRIES = config.etl.get('MAX_RETRIES', 0)


class EmrActivity(Activity):
    """EMR Activity class
    """

    def __init__(self,
                 id,
                 resource,
                 schedule,
                 input_node,
                 emr_step_string,
                 output_node=None,
                 additional_files=None,
                 max_retries=None,
                 depends_on=None):
        """Constructor for the EmrActivity class

        Args:
            id(str): id of the object
            resource(Ec2Resource / EMRResource): resource to run the activity on
            schedule(Schedule): schedule of the pipeline
            emr_step_string(list of str): command string to be executed
            output_node(S3Node): output_node for the emr job
            additional_files(list of S3File): Additional files required for emr
            max_retries(int): number of retries for the activity
            depends_on(list of activities): dependendent pipelines steps
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        # Set default values
        if depends_on is None:
            depends_on = []
        if max_retries is None:
            max_retries = MAX_RETRIES

        super(EmrActivity, self).__init__(
            id=id,
            type='EmrActivity',
            maximumRetries=max_retries,
            dependsOn=depends_on,
            runsOn=resource,
            schedule=schedule,
            step=emr_step_string,
            output=output_node,
            input=input_node,
        )

        if additional_files:
            self.add_additional_files(additional_files)
