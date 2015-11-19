"""ETL step wrapper for sql command for inserting into tables
"""
from ..database import SqlScript
from ..database import Table
from ..s3 import S3File
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from .transform import TransformStep


class CreateUpdateSqlStep(TransformStep):
    """Create and Insert step that creates a table and then uses the query to
    update the table data with any sql query provided
    """

    def __init__(self,
                 table_definition,
                 script=None,
                 command=None,
                 analyze_table=True,
                 script_arguments=None,
                 non_transactional=False,
                 **kwargs):
        """Constructor for the CreateUpdateStep class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(command, script):
            raise ETLInputError('Both command and script found')

        # Create S3File with script / command provided
        if script:
            update_script = SqlScript(filename=parse_path(script))
        else:
            update_script = SqlScript(command)
        self.s3_source_dir = kwargs['s3_source_dir']
        sql_script = self.create_script(S3File(text=update_script.sql()))
        sql_script.upload_to_s3()

        dest = Table(SqlScript(filename=parse_path(table_definition)))

        arguments = [
            '--table_definition=%s' % dest.sql().sql(),
            '--sql=%s' % sql_script.s3_path.uri
        ]

        if analyze_table:
            arguments.append('--analyze')

        if non_transactional:
            arguments.append('--non_transactional')

        if script_arguments is not None:
            if not isinstance(script_arguments, list):
                raise ETLInputError(
                    'Script arguments for SQL steps should be a list')
            arguments.extend(script_arguments)

        super(CreateUpdateSqlStep, self).__init__(
            command=const.SQL_RUNNER_COMMAND, script_arguments=arguments,
            no_output=True, **kwargs)

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        cls.pop_inputs(step_args)

        return step_args
