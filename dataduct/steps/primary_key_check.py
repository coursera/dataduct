"""
ETL step wrapper for PK check step can be executed on Ec2 resource
"""
import os

from .qa_transform import QATransformStep
from ..database import Table
from ..database import SqlStatement
from ..config import Config
from ..utils import constants as const
from ..utils.helpers import parse_path

config = Config()


class PrimaryKeyCheckStep(QATransformStep):
    """PrimaryKeyCheckStep class that checks a table for PK violations
    """

    def __init__(self, id, table_definition, script_arguments=None,
                 log_to_s3=False, **kwargs):
        """Constructor for the PrimaryKeyCheckStep class

        Args:
            table_definition(file): table definition for the table to check
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        with open(parse_path(table_definition)) as f:
            table_def_string = f.read()

        if script_arguments is None:
            script_arguments = list()

        # We initialize the table object to check valid strings
        script_arguments.append(
            '--table=%s' % Table(SqlStatement(table_def_string)).sql())

        if log_to_s3:
            script_arguments.append('--log_to_s3')

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.PK_CHECK_SCRIPT_PATH)

        super(PrimaryKeyCheckStep, self).__init__(
            id=id, script=script, script_arguments=script_arguments, **kwargs)
