"""
ETL step wrapper to extract data from RDS to S3
"""
from re import findall

from .etl_step import ETLStep
from ..constants import MYSQL_CONFIG
from ..pipeline.copy_activity import CopyActivity
from ..pipeline.mysql_node import MysqlNode
from ..pipeline.pipeline_object import PipelineObject
from ..pipeline.shell_command_activity import ShellCommandActivity
from ..utils.helpers import exactly_one
from ..utils.exceptions import ETLInputError


def guess_input_tables(sql):
    """Guess input tables from the sql query
    Returns:
        results(list of str): tables which are used in the sql statement
    """
    results = findall(r'from ([A-Za-z0-9._]+)', sql)
    results.extend(findall(r'FROM ([A-Za-z0-9._]+)', sql))
    results.extend(findall(r'join ([A-Za-z0-9._]+)', sql))
    results.extend(findall(r'JOIN ([A-Za-z0-9._]+)', sql))
    return list(set(results))


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
        if not exactly_one(table, sql):
            raise ETLInputError('Only one of table, sql needed')

        super(ExtractRdsStep, self).__init__(**kwargs)

        if depends_on is not None:
            self._depends_on = depends_on

        if table:
            sql = 'select * from %s;' % table
        elif sql:
            table = guess_input_tables(sql)
        else:
            raise ETLInputError('Provide a sql statement or a table name')

        host = MYSQL_CONFIG[host_name]['HOST']
        user = MYSQL_CONFIG[host_name]['USERNAME']
        password = MYSQL_CONFIG[host_name]['PASSWORD']

        input_node = self.create_pipeline_object(
            object_class=MysqlNode,
            schedule=self.schedule,
            host=host,
            database=database,
            table=table,
            username=user,
            password=password,
            sql=sql,
        )


        s3_format = self.create_pipeline_object(
            object_class=PipelineObject,
            type='TSV'
        )

        intermediate_node = self.create_s3_data_node(format=s3_format)

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
