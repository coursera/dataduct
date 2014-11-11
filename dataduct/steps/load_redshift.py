"""
ETL step wrapper for RedshiftCopyActivity to load data into Redshift
"""
from .etl_step import ETLStep
from ..pipeline.redshift_node import RedshiftNode
from ..pipeline.redshift_copy_activity import RedshiftCopyActivity


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
                 depends_on=None,
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

        if depends_on is not None:
            self._depends_on = depends_on

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
            input_node=self._input_node,
            output_node=self._output,
            insert_mode=insert_mode,
            resource=self.resource,
            schedule=self.schedule,
            depends_on=self.depends_on,
            command_options=command_options,
        )
