"""
ETL step wrapper for creating an S3 node for input from local files
"""
from .etl_step import ETLStep
from ..s3 import S3File


class ExtractLocalStep(ETLStep):
    """ExtractLocal Step class that helps get data from a local file
    """

    def __init__(self, path, **kwargs):
        """Constructor for the ExtractLocalStep class

        Args:
            path(str): local path for data
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(ExtractLocalStep, self).__init__(**kwargs)
        self._output = self.create_s3_data_node(s3_object=S3File(path=path))
