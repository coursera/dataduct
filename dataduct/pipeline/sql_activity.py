"""
Pipeline object class for SqlActivity
"""

from .activity import Activity
from ..config import Config
from .schedule import Schedule
from ..s3 import S3File
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
MAX_RETRIES = config.etl.get('MAX_RETRIES', const.ZERO)
RETRY_DELAY = config.etl.get('RETRY_DELAY', const.DEFAULT_DELAY)


class SqlActivity(Activity):
    """Sql Activity class
    """

    def __init__(self,
                 id,
                 schedule,
                 script,
                 database,
                 resource=None,
                 worker_group=None,
                 script_arguments=None,
                 queue=None,
                 max_retries=None,
                 depends_on=None):
        """Constructor for the SqlActivity class

        Args:
            id(str): id of the object
            schedule(Schedule): schedule of the pipeline
            script(S3File): s3 uri of the script
            database(RedshiftDatabase): database to execute commands on
            resource(Ec2Resource / EMRResource): resource to run the activity on
            worker_group(str): the worker group to run the activity on
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
            max_retries = MAX_RETRIES

        super(SqlActivity, self).__init__(
            id=id,
            retryDelay=RETRY_DELAY,
            type='SqlActivity',
            maximumRetries=max_retries,
            dependsOn=depends_on,
            runsOn=resource,
            workerGroup=worker_group,
            schedule=schedule,
            scriptUri=script,
            scriptArgument=script_arguments,
            database=database,
            queue=queue
        )
