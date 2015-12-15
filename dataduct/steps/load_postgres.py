"""
ETL step wrapper for SQLActivity to load data into Postgres
"""
from .etl_step import ETLStep
from ..pipeline import PostgresNode
from ..pipeline import ShellCommandActivity


class LoadPostgresStep(ETLStep):
    """Load Postgres Step class that helps load data into postgres
    """

    def __init__(self,
                 table=None,
                 sql=None,
                 database=None,
                 output_path=None,
                 **kwargs):
        """Constructor for the LoadPostgresStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            insert_mode(str): insert mode for redshift copy activity
            redshift_database(RedshiftDatabase): database to excute the query
            max_errors(int): Maximum number of errors to be ignored during load
            replace_invalid_char(char): char to replace not utf-8 with
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(LoadPostgresStep, self).__init__(**kwargs)

        # Create output node
        self._output = self.create_pipeline_object(
            object_class=PostgresNode,
            schedule=self.schedule,
            database=database,
            table=table,
            username=user,
            password=password,
            sql=sql,
        )

        command_options = ["DELIMITER '\t' ESCAPE TRUNCATECOLUMNS"]
        command_options.append("NULL AS 'NULL' ")
        if max_errors:
            command_options.append('MAXERROR %d' % int(max_errors))
        if replace_invalid_char:
            command_options.append(
                "ACCEPTINVCHARS AS '%s'" %replace_invalid_char)

        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            input_node=self.input,
            output_node=self.output,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['redshift_database'] = etl.redshift_database

        return step_args
