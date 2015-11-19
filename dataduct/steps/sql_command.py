"""
ETL step wrapper for SqlActivity can be executed on Ec2
"""
from .etl_step import ETLStep
from ..pipeline import SqlActivity
from ..database import SqlScript
from ..s3 import S3File
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from ..utils.exceptions import ETLInputError

import logging
logger = logging.getLogger(__name__)


class SqlCommandStep(ETLStep):
    """SQL Command Step class that helps run scripts on resouces
    """

    def __init__(self,
                 redshift_database,
                 script=None,
                 script_arguments=None,
                 queue=None,
                 sql_script=None,
                 command=None,
                 wrap_transaction=True,
                 **kwargs):
        """Constructor for the SqlCommandStep class

        Args:
            command(str): command to be executed directly
            script(path): local path to the script that should executed
            queue(str): query queue that should be used
            script_arguments(list of str): arguments to the SQL command
            redshift_database(RedshiftDatabase): database to excute the query
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(command, script, sql_script):
            raise ETLInputError('Both command and script found')

        if sql_script is not None and not isinstance(sql_script, SqlScript):
            raise ETLInputError('sql_script should be of the type SqlScript')

        super(SqlCommandStep, self).__init__(**kwargs)

        # Create S3File with script / command provided
        if script:
            sql_script = SqlScript(filename=parse_path(script))
        elif command:
            sql_script = SqlScript(command)

        if wrap_transaction:
            sql_script = sql_script.wrap_transaction()

        script = self.create_script(S3File(text=sql_script.sql()))

        logger.debug('Sql Query:')
        logger.debug(sql_script)

        self.create_pipeline_object(
            object_class=SqlActivity,
            max_retries=self.max_retries,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            database=redshift_database,
            script_arguments=script_arguments,
            depends_on=self.depends_on,
            script=script,
            queue=queue,
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        input_args = cls.pop_inputs(input_args)
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['redshift_database'] = etl.redshift_database

        return step_args
