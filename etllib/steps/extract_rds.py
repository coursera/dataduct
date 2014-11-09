"""
ETL step wrapper to extract data from RDS to S3
"""
from .etl_step import ETLStep
from ..pipeline.copy_activity import CopyActivity
from ..pipeline.mysql_node import MysqlNode
from ..pipeline.shell_command_activity import ShellCommandActivity
from ..s3.s3_file import S3File
from ..utils.exceptions import ETLInputError

class ExtractRdsStep(ETLStep):
    """Extract Redshift Step class that helps get data out of redshift
    """

    def __init__(self,
                 table=None,
                 sql=None,
                 host_name=None,
                 database=None,
                 depends_on=None,
                 **kwargs):
        """Constructor for the ExtractRdsStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            insert_mode(str): insert mode for redshift copy activity
            redshift_database(RedshiftDatabase): database to excute the query
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(ExtractRdsStep, self).__init__(**kwargs)

        if depends_on is not None:
            self.depends_on = depends_on

        if table:
            script = S3File(text='select * from %s;' % table)
        elif sql:
            script = S3File(path=sql)
        else:
            raise ETLInputError('Provide a sql statement or a table name')

        input_node = self.create_pipeline_object(
            object_class=MysqlNode,
            schedule=self.schedule,
            host=host_name,
            database=database,
            table=table,
            user=None,
            password=None,
            sql=script,
        )
        # TODO: Config for username and password

        intermediate_node = self.create_s3_data_node()

        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            input_node=input_node,
            output_node=intermediate_node,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )

        # This shouldn't be necessary but -
        # AWS uses \\n as null, so we need to remove it
        self._output = self.create_s3_data_node()

        command = ' '.join(["cat",
                            "${INPUT1_STAGING_DIR}/*",
                            "| sed 's/\\\\\\\\n/NULL/g'",  # replace \\n
                            # get rid of control characters
                            "| tr -d '\\\\000'",
                            "> ${OUTPUT1_STAGING_DIR}/part-0"]
                          )

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=intermediate_node,
            output_node=self._output,
            command=command,
            max_retries=self.max_retries,
            resource=self.resource,
            schedule=self.schedule,
        )
