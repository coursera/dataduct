"""ETL step wrapper for column check step can be executed on Ec2 resource
"""
import os

from .qa_transform import QATransformStep
from ..config import Config
from ..utils import constants as const
from ..utils.helpers import parse_path

config = Config()


class ColumnCheckStep(QATransformStep):
    """ColumnCheckStep class that checks if the rows of a table has been
    populated with the correct values
    """

    def __init__(self, id, source_table_definition,
                 destination_table_definition, **kwargs):
        """Constructor for the ColumnCheckStep class

        Args:
            source_table_definition(file):
                table definition for the source table
            destination_table_definition(file):
                table definition for the destination table
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        with open(parse_path(source_table_definition)) as f:
            source_table_string = f.read()
        with open(parse_path(destination_table_definition)) as f:
            destination_table_string = f.read()

        script_arguments = ['--source_table=%s' % source_table_string,
                            '--destination_table=%s'
                            % destination_table_string]

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.COLUMN_CHECK_SCRIPT_PATH)

        super(ColumnCheckStep, self).__init__(
            id=id, script=script, script_arguments=script_arguments, **kwargs)
