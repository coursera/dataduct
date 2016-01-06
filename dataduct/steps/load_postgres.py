"""
ETL step wrapper for SQLActivity to load data into Postgres
"""
from ..config import Config
from .etl_step import ETLStep
from ..pipeline import PostgresNode
from ..pipeline import PostgresDatabase
from ..pipeline import PipelineObject
from ..pipeline import CopyActivity

config = Config()
if not hasattr(config, 'postgres'):
    raise ETLInputError('Postgres config not specified in ETL')
POSTGRES_CONFIG = config.postgres


class LoadPostgresStep(ETLStep):
    """Load Postgres Step class that helps load data into postgres
    """

    def __init__(self,
                 table,
                 postgres_database,
                 insert_query,
                 max_errors=None,
                 replace_invalid_char=None,
                 **kwargs):
        """Constructor for the LoadPostgresStep class

        Args:
            table(path): table name for load
            sql(str): sql query to be executed
            postgres_database(PostgresDatabase): database to excute the query
            output_path(str): s3 path where sql output should be saved
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(LoadPostgresStep, self).__init__(**kwargs)

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

        # Create output node
        self._output = self.create_pipeline_object(
            object_class=PostgresNode,
            schedule=self.schedule,
            database=database_node,
            table=table,
            username=user,
            password=password,
            select_query=None,
            insert_query=insert_query,
            host=rds_instance_id,
        )

        self.create_pipeline_object(
            object_class=CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            input_node=self.input,
            output_node=self.output,
            depends_on=self.depends_on,
            max_retries=self.max_retries,
        )

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['postgres_database'] = etl.postgres_database

        return step_args
