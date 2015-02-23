"""ETL step wrapper for Upsert SQL script
"""
from .etl_step import ETLStep
from ..pipeline import SqlActivity
from ..database import Table
from ..database import SqlScript
from ..database import SelectStatement
from ..database import HistoryTable
from ..s3 import S3File
from ..utils.helpers import parse_path
from ..utils.helpers import exactly_one


class UpsertStep(ETLStep):
    """Upsert Step class that helps run a step on the emr cluster
    """

    def __init__(self, destination, redshift_database, sql=None,
                 script=None, source=None, enforce_primary_key=True,
                 delete_existing=False, history=None, **kwargs):
        """Constructor for the UpsertStep class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        assert exactly_one(sql, source, script), 'One of sql/source/script'
        super(UpsertStep, self).__init__(**kwargs)

        # Input formatting
        dest = Table(SqlScript(filename=parse_path(destination)))
        if source is not None:
            source_relation = Table(SqlScript(filename=parse_path(source)))
        else:
            source_relation = SelectStatement(
                SqlScript(sql=sql, filename=script).sql())

        # Create the destination table if doesn't exist
        script = dest.exists_clone_script()
        script.append(dest.upsert_script(
            source_relation, enforce_primary_key, delete_existing))

        if history:
            hist = HistoryTable(SqlScript(
                filename=parse_path(history)))
            script.append(hist.update_history_script(dest))

        self.activity = self.create_pipeline_object(
            object_class=SqlActivity,
            resource=self.resource,
            schedule=self.schedule,
            depends_on=self.depends_on,
            database=redshift_database,
            max_retries=self.max_retries,
            script=self.create_script(S3File(text=script.sql())))

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        cls.pop_inputs(step_args)
        step_args['resource'] = etl.ec2_resource
        step_args['redshift_database'] = etl.redshift_database
        return step_args
