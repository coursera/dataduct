"""ETL step wrapper for loading into redshift with the COPY command
"""
from ..config import Config
from ..database import SqlStatement
from ..database import Table
from ..utils import constants as const
from ..utils.helpers import parse_path
from .transform import TransformStep

config = Config()


class CreateAndLoadStep(TransformStep):
    """CreateAndLoad Step class that creates table if needed and loads data
    """

    def __init__(self, id, table_definition, input_node,
                 script_arguments=None, **kwargs):
        """Constructor for the CreateAndLoadStep class

        Args:
            table_definition(filepath): schema file for the table to be loaded
            script_arguments(list of str): list of arguments to the script
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        with open(parse_path(table_definition)) as f:
            table_def_string = f.read()

        table = Table(SqlStatement(table_def_string))

        if isinstance(input_node, dict):
            input_paths = [i.path().uri for i in input_node.values()]
        else:
            input_paths = [input_node.path().uri]

        if script_arguments is None:
            script_arguments = list()

        script_arguments.extend([
            '--table_definition=%s' % table.sql().sql(),
            '--s3_input_paths'] + input_paths)

        super(CreateAndLoadStep, self).__init__(
            id=id, command=const.LOAD_COMMAND,
            script_arguments=script_arguments, no_input=True, no_output=True,
            **kwargs)

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)

        return step_args
