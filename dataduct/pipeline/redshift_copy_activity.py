"""
Pipeline object class for RedshiftCopyActivity
"""

from .activity import Activity
from .redshift_node import RedshiftNode
from .schedule import Schedule
from ..constants import DEFAULT_MAX_RETRIES
from ..constants import RETRY_DELAY
from ..utils.exceptions import ETLInputError


class RedshiftCopyActivity(Activity):
    """EMR Activity class
    """

    def __init__(self,
                 id,
                 resource,
                 schedule,
                 input_node,
                 output_node,
                 insert_mode,
                 command_options=None,
                 max_retries=None,
                 depends_on=None):
        """Constructor for the RedshiftCopyActivity class

        Args:
            id(str): id of the object
            resource(Ec2Resource / EMRResource): resource to run the activity on
            schedule(Schedule): schedule of the pipeline
            input_node(S3Node / RedshiftNode): input data node
            output_node(S3Node / RedshiftNode): output data node
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
            max_retries = DEFAULT_MAX_RETRIES

        kwargs = {
            'id': id,
            'retryDelay': RETRY_DELAY,
            'type': 'RedshiftCopyActivity',
            'maximumRetries': max_retries,
            'input': input_node,
            'output': output_node,
            'runsOn': resource,
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
