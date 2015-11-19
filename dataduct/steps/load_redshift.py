"""
ETL step wrapper for RedshiftCopyActivity to load data into Redshift
"""
from .etl_step import ETLStep
from ..pipeline import RedshiftNode
from ..pipeline import RedshiftCopyActivity


class LoadRedshiftStep(ETLStep):
    """Load Redshift Step class that helps load data into redshift
    """

    def __init__(self,
                 schema,
                 table,
                 redshift_database,
                 insert_mode="TRUNCATE",
                 max_errors=None,
                 replace_invalid_char=None,
                 **kwargs):
        """Constructor for the LoadRedshiftStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            insert_mode(str): insert mode for redshift copy activity
            redshift_database(RedshiftDatabase): database to excute the query
            max_errors(int): Maximum number of errors to be ignored during load
            replace_invalid_char(char): char to replace not utf-8 with
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(LoadRedshiftStep, self).__init__(**kwargs)

        # Create output node
        self._output = self.create_pipeline_object(
            object_class=RedshiftNode,
            schedule=self.schedule,
            redshift_database=redshift_database,
            schema_name=schema,
            table_name=table,
        )

        command_options = ["DELIMITER '\t' ESCAPE TRUNCATECOLUMNS"]
        command_options.append("NULL AS 'NULL' ")
        if max_errors:
            command_options.append('MAXERROR %d' % int(max_errors))
        if replace_invalid_char:
            command_options.append(
                "ACCEPTINVCHARS AS '%s'" %replace_invalid_char)

        self.create_pipeline_object(
            object_class=RedshiftCopyActivity,
            max_retries=self.max_retries,
            input_node=self.input,
            output_node=self.output,
            insert_mode=insert_mode,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            depends_on=self.depends_on,
            command_options=command_options,
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
