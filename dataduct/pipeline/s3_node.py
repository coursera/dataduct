"""
Pipeline object class for S3Node
"""

from ..config import Config
from .pipeline_object import PipelineObject
from .precondition import Precondition
from .schedule import Schedule

from ..s3 import S3Path
from ..s3 import S3File
from ..s3 import S3Directory
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
RETRY_DELAY = config.etl.get('RETRY_DELAY', const.DEFAULT_DELAY)


class S3Node(PipelineObject):
    """S3 Data Node class
    """

    def __init__(self,
                 id,
                 schedule,
                 s3_object,
                 precondition=None,
                 format=None,
                 **kwargs):
        """Constructor for the S3Node class

        Args:
            id(str): id of the object
            schedule(Schedule): pipeline schedule
            s3_object(S3Path / S3File / S3Directory): s3 location
            precondition(Precondition): precondition to the data node
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        if precondition and not isinstance(precondition, Precondition):
            raise ETLInputError(
                'Input precondition must be of the type Precondition')

        if not(isinstance(s3_object, S3Path) or
               isinstance(s3_object, S3File) or
               isinstance(s3_object, S3Directory)):
            raise ETLInputError('Mismatched type for S3 path')

        additional_args = {}
        if (isinstance(s3_object, S3Path) and s3_object.is_directory) or \
            (isinstance(s3_object, S3Directory)):
            additional_args['directoryPath'] = s3_object
        else:
            additional_args['filePath'] = s3_object

        # Save the s3_object variable
        self._s3_object = s3_object

        # Save the dependent nodes from the S3 Node
        self._dependency_nodes = list()

        super(S3Node, self).__init__(
            id=id,
            retryDelay=RETRY_DELAY,
            type='S3DataNode',
            schedule=schedule,
            dataFormat=format,
            precondition=precondition,
            **additional_args
        )


    def path(self):
        """Get the s3_object associated with the S3 data node

        Returns:
            s3_object(S3Path): The s3 path of the node can a directory or file
        """
        if isinstance(self._s3_object, S3File):
            return self._s3_object.s3_path
        else:
            return self._s3_object

    @property
    def dependency_nodes(self):
        """Fetch the dependent nodes for the S3 node
        """
        return self._dependency_nodes

    def add_dependency_node(self, input_node):
        """Add nodes to the list of dependencies among S3 Nodes
        """
        self._dependency_nodes.append(input_node)
