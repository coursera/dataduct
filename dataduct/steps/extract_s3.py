"""
ETL step wrapper for creating an S3 node for input
"""
from ..s3 import S3Path
from ..utils.exceptions import ETLInputError
from ..utils.helpers import exactly_one
from ..utils.helpers import get_modified_s3_path
from .etl_step import ETLStep


class ExtractS3Step(ETLStep):
    """ExtractS3 Step class that helps get data from S3
    """

    def __init__(self, directory_uri=None, file_uri=None, **kwargs):
        """Constructor for the ExtractS3Step class

        Args:
            directory_uri(str): s3 path for s3 data directory
            file_uri(str): s3 path for s3 data file
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(directory_uri, file_uri):
            raise ETLInputError('One of file_uri or directory_uri needed')

        super(ExtractS3Step, self).__init__(**kwargs)

        if directory_uri:
            directory_uri = get_modified_s3_path(directory_uri)
            s3_path = S3Path(uri=directory_uri, is_directory=True)
        else:
            file_uri = get_modified_s3_path(file_uri)
            s3_path = S3Path(uri=file_uri)
        self._output = self.create_s3_data_node(s3_path)

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args.pop('resource', None)
        step_args.pop('worker_group', None)

        return step_args
