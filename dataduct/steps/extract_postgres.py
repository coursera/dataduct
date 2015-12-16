"""
ETL step wrapper to extract data from RDS to S3
"""
from ..config import Config
from .etl_step import ETLStep
from ..pipeline import CopyActivity
from ..pipeline import PostgresNode
from ..pipeline import PostgresDatabase
from ..pipeline import PipelineObject
from ..pipeline import ShellCommandActivity
from ..utils.helpers import exactly_one
from ..utils.exceptions import ETLInputError
from ..database import SelectStatement

config = Config()
if not hasattr(config, 'postgres'):
    raise ETLInputError('Postgres config not specified in ETL')

POSTGRES_CONFIG = config.postgres


class ExtractPostgresStep(ETLStep):
    """Extract Postgres Step class that helps get data out of postgres
    """

    def __init__(self,
                 table=None,
                 sql=None,
                 output_path=None,
                 **kwargs):
        """Constructor for the ExtractPostgresStep class

        Args:
            schema(str): schema from which table should be extracted
            table(path): table name for extract
            insert_mode(str): insert mode for redshift copy activity
            redshift_database(RedshiftDatabase): database to excute the query
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(table, sql):
            raise ETLInputError('Only one of table, sql needed')

        super(ExtractPostgresStep, self).__init__(**kwargs)

        if table:
            sql = 'SELECT * FROM %s;' % table
        elif sql:
            table = SelectStatement(sql).dependencies[0]
        else:
            raise ETLInputError('Provide a sql statement or a table name')

        region = POSTGRES_CONFIG['REGION']
        rds_instance_id = POSTGRES_CONFIG['RDS_INSTANCE_ID']
        user = POSTGRES_CONFIG['USERNAME']
        password = POSTGRES_CONFIG['PASSWORD']

        database_node = self.create_pipeline_object(
                    object_class=PostgresDatabase,
                    region=region,
                    rds_instance_id=rds_instance_id,
                    username=user,
                    password=password,
        )

        input_node = self.create_pipeline_object(
            object_class=PostgresNode,
            schedule=self.schedule,
            database=database_node,
            table=table,
            username=user,
            password=password,
            sql=sql,
            host=rds_instance_id,
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

        self._output = self.create_s3_data_node(
            self.get_output_s3_path(output_path))

        # This shouldn't be necessary but -
        # AWS uses \\n as null, so we need to remove it
        command = ' '.join(["cat",
                            "${INPUT1_STAGING_DIR}/*",
                            "| sed 's/\\\\\\\\n/NULL/g'",  # replace \\n
                            # get rid of control characters
                            "| tr -d '\\\\000'",
                            "> ${OUTPUT1_STAGING_DIR}/part-0"])

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=intermediate_node,
            output_node=self.output,
            command=command,
            max_retries=self.max_retries,
            resource=self.resource,
            schedule=self.schedule,
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
        step_args['resource'] = etl.ec2_resource

        return step_args
