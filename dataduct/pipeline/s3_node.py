"""
Pipeline object class for S3Node
"""

from .pipeline_object import PipelineObject
from .precondition import Precondition
from .schedule import Schedule

from ..constants import RETRY_DELAY
from ..s3.s3_path import S3Path
from ..s3.s3_file import S3File
from ..s3.s3_directory import S3Directory
from ..utils.exceptions import ETLInputError


class S3Node(PipelineObject):
    """S3 Data Node class
    """

    def __init__(self,
                 id,
                 schedule,
                 s3_path,
                 precondition=None,
                 format=None,
                 **kwargs):
        """Constructor for the S3Node class

        Args:
            id(str): id of the object
            schedule(Schedule): pipeline schedule
            s3_path(S3Path / S3File / S3Directory): s3 location
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

        if not(isinstance(s3_path, S3Path) or
               isinstance(s3_path, S3File) or
               isinstance(s3_path, S3Directory)):
            raise ETLInputError('Mismatched type for S3 path')

        additional_args = {}
        if isinstance(s3_path, S3Path) and s3_path.is_directory:
            additional_args['directoryPath'] = s3_path
        else:
            additional_args['filePath'] = s3_path

        # Save the s3_path variable
        self._s3_path = s3_path

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
        """Get the s3_path associated with the S3 data node
        Returns:
            s3_path(S3Path): The s3 path of the node can a directory or file
        """
        if isinstance(self._s3_path, S3File):
            return self._s3_path.s3_path
        else:
            return self._s3_path
