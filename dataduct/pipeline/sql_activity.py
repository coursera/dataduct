"""
Pipeline object class for SqlActivity
"""

from .activity import Activity
from ..config import Config
from .schedule import Schedule
from ..s3 import S3File

from ..utils.exceptions import ETLInputError

config = Config()
DEFAULT_MAX_RETRIES = config.etl['DEFAULT_MAX_RETRIES']
RETRY_DELAY = config.etl['RETRY_DELAY']


class SqlActivity(Activity):
    """Sql Activity class
    """

    def __init__(self,
                 id,
                 resource,
                 schedule,
                 script,
                 database,
                 script_arguments=None,
                 queue=None,
                 max_retries=None,
                 depends_on=None):
        """Constructor for the SqlActivity class

        Args:
            id(str): id of the object
            resource(Ec2Resource / EMRResource): resource to run the activity on
            schedule(Schedule): schedule of the pipeline
            script(S3File): s3 uri of the script
            script_arguments(list of str): command line arguments to the script
            database(RedshiftDatabase): database to execute commands on
            queue(str): queue in which the query should be executed
            max_retries(int): number of retries for the activity
            depends_on(list of activities): dependendent pipelines steps
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        if not isinstance(script, S3File):
            raise ETLInputError('script must be an S3File')

        # Set default values
        if depends_on is None:
            depends_on = []
        if max_retries is None:
            max_retries = DEFAULT_MAX_RETRIES

        super(SqlActivity, self).__init__(
            id=id,
            retryDelay=RETRY_DELAY,
            type='SqlActivity',
            maximumRetries=max_retries,
            dependsOn=depends_on,
            runsOn=resource,
            schedule=schedule,
            scriptUri=script,
            scriptArgument=script_arguments,
            database=database,
            queue=queue
        )
