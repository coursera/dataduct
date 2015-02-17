"""
ETL step wrapper for creating an S3 node for input from local files
"""
from dataduct.steps import ExtractLocalStep
import logging
logger = logging.getLogger(__name__)


class CustomExtractLocalStep(ExtractLocalStep):
    """CustomExtractLocal Step class that helps get data from a local file
    """

    def __init__(self, **kwargs):
        """Constructor for the CustomExtractLocal class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        logger.info('Using the Custom Extract Local Step')
        super(CustomExtractLocalStep, self).__init__(**kwargs)
