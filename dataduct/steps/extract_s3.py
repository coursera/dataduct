"""
ETL step wrapper for creating an S3 node for input
"""
from .etl_step import ETLStep
from ..s3.s3_path import S3Path


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
