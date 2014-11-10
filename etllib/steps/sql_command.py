"""
ETL step wrapper for SqlActivity can be executed on Ec2
"""
from .etl_step import ETLStep
from ..pipeline.sql_activity import SqlActivity
from ..s3.s3_file import S3File
from ..utils.helpers import exactly_one
from ..utils.exceptions import ETLInputError


class SqlCommandStep(ETLStep):
    """SQL Command Step class that helps run scripts on resouces
    """

    def __init__(self,
                 redshift_database,
                 script=None,
                 script_arguments=None,
                 queue=None,
                 command=None,
                 depends_on=None,
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
        if not exactly_one(command, script):
            raise ETLInputError('Both command or script found')

        super(SqlCommandStep, self).__init__(**kwargs)

        if depends_on is not None:
            self._depends_on = depends_on

        # Create S3File with script / command provided
        if script:
            script = self.create_script(S3File(path=script))
        else:
            script = self.create_script(S3File(text=command))

        self.create_pipeline_object(
            object_class=SqlActivity,
            max_retries=self.max_retries,
            resource=self.resource,
            schedule=self.schedule,
            database=redshift_database,
            script_arguments=script_arguments,
            depends_on=self.depends_on,
            script=script,
            queue=queue,
        )
