"""
ETL step wrapper for RedshiftCopyActivity to extract data to S3
"""
from .etl_step import ETLStep
from ..pipeline import RedshiftNode
from ..pipeline import RedshiftCopyActivity


class ExtractRedshiftStep(ETLStep):
    """Extract Redshift Step class that helps get data out of redshift
    """

    def __init__(self,
                 schema,
                 table,
                 redshift_database,
                 insert_mode="TRUNCATE",
                 output_path=None,
                 **kwargs):
        """Constructor for the ExtractRedshiftStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            insert_mode(str): insert mode for redshift copy activity
            redshift_database(RedshiftDatabase): database to excute the query
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(ExtractRedshiftStep, self).__init__(**kwargs)

        # Create input node
        self._input_node = self.create_pipeline_object(
            object_class=RedshiftNode,
            schedule=self.schedule,
            redshift_database=redshift_database,
            schema_name=schema,
            table_name=table,
        )

        self._output = self.create_s3_data_node(
            self.get_output_s3_path(output_path))

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
            command_options=["DELIMITER '\t' ESCAPE"],
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
