"""
ETL step wrapper for creating an S3 node for input from local files
"""
from dataduct.steps import ExtractLocalStep


class CustomExtractLocalStep(ExtractLocalStep):
    """CustomExtractLocal Step class that helps get data from a local file
    """

    def __init__(self, **kwargs):
        """Constructor for the CustomExtractLocal class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        print 'Using the Custom Extract Local Step'
        super(CustomExtractLocalStep, self).__init__(**kwargs)
