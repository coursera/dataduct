"""
ETL step wrapper for creating an S3 node for input
"""
from .etl_step import ETLStep
from ..s3 import S3Path


class ExtractS3Step(ETLStep):
    """ExtractS3 Step class that helps get data from S3
    """

    def __init__(self, uri, **kwargs):
        """Constructor for the ExtractS3Step class

        Args:
            uri(str): s3 path for s3 data
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(ExtractS3Step, self).__init__(**kwargs)
        self._output = self.create_s3_data_node(s3_object=S3Path(uri=uri))

    @staticmethod
    def argument_parser(etl, step_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args.pop('resource')
        return step_args
