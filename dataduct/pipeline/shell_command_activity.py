"""
Pipeline object class for ShellCommandActivity
"""

from .activity import Activity
from ..config import Config
from .schedule import Schedule

from ..utils.exceptions import ETLInputError

config = Config()
MAX_RETRIES = config.etl.get('MAX_RETRIES', 0)
RETRY_DELAY = config.etl.get('RETRY_DELAY', '10 Minutes')


class ShellCommandActivity(Activity):
    """ShellCommandActivity class
    """

    def __init__(self,
                 id,
                 input_node,
                 output_node,
                 resource,
                 schedule,
                 script_uri=None,
                 script_arguments=None,
                 command=None,
                 max_retries=None,
                 depends_on=None,
                 additional_s3_files=None):
        """Constructor for the ShellCommandActivity class

        Args:
            id(str): id of the object
            input_node(S3Node / list of S3Nodes): input nodes for the activity
            output_node(S3Node / list of S3Nodes): output nodes for activity
            resource(Ec2Resource / EMRResource): resource to run the activity on
            schedule(Schedule): schedule of the pipeline
            script_uri(S3File): s3 uri of the script
            script_arguments(list of str): command line arguments to the script
            command(str): command to be run as shell activity
            max_retries(int): number of retries for the activity
            depends_on(list of activities): dependendent pipelines steps
            additional_s3_files(list of s3File): additional files for activity
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        if command is not None and script_uri is not None:
            raise ETLInputError('command and script both can not be provided')

        # Set default values
        if depends_on is None:
            depends_on = []
        if max_retries is None:
            max_retries = MAX_RETRIES

        super(ShellCommandActivity, self).__init__(
            id=id,
            retryDelay=RETRY_DELAY,
            type='ShellCommandActivity',
            maximumRetries=max_retries,
            dependsOn=depends_on,
            stage='true',
            input=input_node,
            output=output_node,
            runsOn=resource,
            schedule=schedule,
            scriptUri=script_uri,
            scriptArgument=script_arguments,
            command=command
        )

        # Add the additional s3 files
        self.add_additional_files(additional_s3_files)
