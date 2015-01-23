"""ETL step wrapper for count check step can be executed on the Ec2 resource
"""
import os

from .qa_transform import QATransformStep
from ..config import Config
from ..utils import constants as const
from ..utils.helpers import parse_path

config = Config()


class CountCheckStep(QATransformStep):
    """CountCheckStep class that compares the number of rows in the source
       select script with the number of rows in the destination table
    """

    def __init__(self, id, source_script, destination_table_definition,
                 **kwargs):
        """Constructor for the CountCheckStep class

        Args:
            source_script(str): SQL select script from the source table
            destination_table_definition(file):
                table definition for the destination table
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        with open(parse_path(destination_table_definition)) as f:
            destination_table_string = f.read()

        script_arguments = ['--source_script=%s' % source_script,
                            '--destination_table=%s' %
                            destination_table_string]

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.COUNT_CHECK_SCRIPT_PATH)

        super(CountCheckStep, self).__init__(
            id=id, script=script, script_arguments=script_arguments, **kwargs)
