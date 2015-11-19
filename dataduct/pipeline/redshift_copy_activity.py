"""
Pipeline object class for RedshiftCopyActivity
"""

from .activity import Activity
from ..config import Config
from .redshift_node import RedshiftNode
from .schedule import Schedule
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
MAX_RETRIES = config.etl.get('MAX_RETRIES', const.ZERO)
RETRY_DELAY = config.etl.get('RETRY_DELAY', const.DEFAULT_DELAY)


class RedshiftCopyActivity(Activity):
    """EMR Activity class
    """

    def __init__(self,
                 id,
                 schedule,
                 input_node,
                 output_node,
                 insert_mode,
                 resource=None,
                 worker_group=None,
                 command_options=None,
                 max_retries=None,
                 depends_on=None):
        """Constructor for the RedshiftCopyActivity class

        Args:
            id(str): id of the object
            schedule(Schedule): schedule of the pipeline
            input_node(S3Node / RedshiftNode): input data node
            output_node(S3Node / RedshiftNode): output data node
            resource(Ec2Resource / EMRResource): resource to run the activity on
            worker_group(str): the worker group to run the activity on
            command_options(list of str): command options for the activity
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

        kwargs = {
            'id': id,
            'retryDelay': RETRY_DELAY,
            'type': 'RedshiftCopyActivity',
            'maximumRetries': max_retries,
            'input': input_node,
            'output': output_node,
            'runsOn': resource,
            'workerGroup': worker_group,
            'insertMode': insert_mode,
            'schedule': schedule,
            'dependsOn': depends_on,
            'commandOptions': command_options
        }

        if isinstance(input_node, RedshiftNode):
            # AWS BUG: AWS expects fully qualified name when extracting from
            # Redshift, but not when loading into redshift. Here, we enforce
            # a convention of providing schemaName and tableName separately.
            assert "." not in input_node['tableName'], \
                "Using convention that table name is not fully qualified. " + \
                "Provide the schema name separately from the table name."
            table_name = input_node['tableName']
            del input_node['tableName']
            input_node['tableName'] = "%s.%s" % (input_node['schemaName'],
                                                 table_name)
        super(RedshiftCopyActivity, self).__init__(**kwargs)
