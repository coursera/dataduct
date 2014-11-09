"""
ETL step wrapper for RedshiftCopyActivity to extract data to S3
"""
from .etl_step import ETLStep
from ..pipeline.redshift_node import RedshiftNode
from ..pipeline.redshift_copy_activity import RedshiftCopyActivity


class ExtractRedshiftStep(ETLStep):
    """Extract Redshift Step class that helps get data out of redshift
    """

    def __init__(self,
                 schema,
                 table,
                 redshift_database,
                 insert_mode="TRUNCATE",
                 depends_on=None,
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

        if depends_on is not None:
            self._depends_on = depends_on

        # Create input node
        self._input_node = self.create_pipeline_object(
            object_class=RedshiftNode,
            schedule=self.schedule,
            redshift_database=redshift_database,
            schema_name=schema,
            table_name=table,
        )

        self._output = self.create_s3_data_node()

        self.create_pipeline_object(
            object_class=RedshiftCopyActivity,
            max_retries=self.max_retries,
            input_node=self._input_node,
            output_node=self._output,
            insert_mode=insert_mode,
            resource=self.resource,
            schedule=self.schedule,
            depends_on=self.depends_on,
            command_options=["DELIMITER '\t' ESCAPE"],
        )
