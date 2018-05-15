"""
ETL step wrapper for PK check step can be executed on Ec2 resource
"""
from ..config import Config
from ..database import SqlStatement
from ..database import Table
from ..utils import constants as const
from ..utils.helpers import parse_path
from .qa_transform import QATransformStep

config = Config()


class PrimaryKeyCheckStep(QATransformStep):
    """PrimaryKeyCheckStep class that checks a table for PK violations
    """

    def __init__(self, id, table_definition, script_arguments=None,
                 log_to_s3=False, command=None, script=None, **kwargs):
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

        if script is None and command is None:
            command = const.PK_CHECK_COMMAND

        super(PrimaryKeyCheckStep, self).__init__(
            id=id, command=command, script=script,
            script_arguments=script_arguments, **kwargs)
