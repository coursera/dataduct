"""ETL step wrapper for delta loading a table based on a date column
"""
from ..config import Config
from ..database import SelectStatement
from ..database import SqlScript
from ..database import SqlStatement
from ..database import Table
from ..pipeline import ShellCommandActivity
from ..utils import constants as const
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from .etl_step import ETLStep

config = Config()


class DeltaLoadStep(ETLStep):
    """DeltaLoadStep Step class that creates tables if needed and loads data
    """

    def __init__(self, id, staging_table_definition,
                 production_table_definition, date_column, pipeline_name,
                 sql=None, script=None, source=None, window=0,
                 script_arguments=None, analyze_table=True,
                 enforce_primary_key=True, log_to_s3=False, **kwargs):
        """Constructor for the DeltaLoadStep class

        Args:
            staging_table_definition(filepath):
                schema file for temporary data storage
            production_table_definition(filepath):
                schema file for the table to be delta loaded into
            date_column(string): name of column (of type date) to use as the
                delta value (i.e., only load the last X days)
            window(int): number of days before last loaded day to update
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(DeltaLoadStep, self).__init__(id=id, **kwargs)

        # TODO: Move the following four steps into lib after
        # they support all the parameters
        ensure_exists_pipeline_object = self.ensure_exists(
            production=production_table_definition
        )

        reload_pipeline_object = self.delta_reload(
            sql=sql,
            script=script,
            source=source,
            staging=staging_table_definition,
            production=production_table_definition,
            date_column=date_column,
            window=window,
            depends_on=[ensure_exists_pipeline_object],
            enforce_primary_key=enforce_primary_key
        )

        upsert_pipeline_object = self.upsert(
            source=staging_table_definition,
            destination=production_table_definition,
            depends_on=[reload_pipeline_object],
            analyze_table=analyze_table,
            enforce_primary_key=enforce_primary_key
        )

        self.primary_key_check(
            table_definition=production_table_definition,
            pipeline_name=pipeline_name,
            depends_on=[upsert_pipeline_object],
            log_to_s3=log_to_s3
        )

    def primary_key_check(self, table_definition, pipeline_name, depends_on,
                          log_to_s3):
        table = self.get_table_from_def(table_definition)

        # We initialize the table object to check valid strings
        script_arguments = ['--table=%s' % table.sql()]

        if log_to_s3:
            script_arguments.append('--log_to_s3')

        step_name = self.get_name('primary_key_check')
        script_arguments.append(
            '--test_name=%s' % (pipeline_name + '.' + step_name))

        sns_topic_arn = config.etl.get('SNS_TOPIC_ARN_WARNING', None)
        if sns_topic_arn:
            script_arguments.append('--sns_topic_arn=%s' % sns_topic_arn)

        primary_key_check_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=step_name,
            input_node=None,
            output_node=None,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            command=const.PK_CHECK_COMMAND,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=depends_on
        )
        return primary_key_check_pipeline_object

    def ensure_exists(self, production):
        dest = Table(SqlScript(filename=parse_path(production)))
        dummy_script = SqlScript('SELECT 1')
        script_arguments = [
            '--table_definition=%s' % dest.sql(),
            '--sql=%s' % dummy_script.sql()
        ]
        ensure_exists_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=self.get_name('ensure_exists'),
            input_node=None,
            output_node=None,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            command=const.SQL_RUNNER_COMMAND,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
        )
        return ensure_exists_pipeline_object

    def delta_reload(self, sql, script, source, staging, production, date_column,
                   window, depends_on, enforce_primary_key):
        assert exactly_one(sql, source, script), 'One of sql/source/script'

        # Input formatting
        dest = Table(SqlScript(filename=parse_path(staging)))
        prod = Table(SqlScript(filename=parse_path(production)))

        if source is not None:
            source_relation = Table(SqlScript(filename=parse_path(source)))
        else:
            source_relation = SelectStatement(
                SqlScript(sql=sql, filename=parse_path(script)).sql())

        delta_clause = """
            WHERE {date_column} >=
                COALESCE(
                    (SELECT MAX({date_column}) FROM {production}),
                    '2000-01-01'::DATE
                ) - {window}
        """.format(date_column=date_column,
                   production=prod.full_name,
                   window=window)

        # Create the destination table if doesn't exist
        reload_script = dest.upsert_script(source_relation, enforce_primary_key,
                                        True, delta_clause)
        script_arguments = [
            '--table_definition=%s' % dest.sql(),
            '--sql=%s' % reload_script.sql()
        ]

        reload_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=self.get_name('reload'),
            input_node=None,
            output_node=None,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            command=const.SQL_RUNNER_COMMAND,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=depends_on
        )
        return reload_pipeline_object

    def upsert(self, source, destination, depends_on,
               analyze_table, enforce_primary_key):
        source_table = parse_path(source)
        destination_table = parse_path(destination)

        source_relation = Table(SqlScript(filename=source_table))
        destination_relation = Table(SqlScript(filename=destination_table))

        sql_script = destination_relation.upsert_script(
            source_relation, enforce_primary_key)

        update_script = SqlScript(sql_script.sql())
        script_arguments = [
            '--table_definition=%s' % destination_relation.sql(),
            '--sql=%s' % update_script.sql()
        ]

        if analyze_table:
            script_arguments.append('--analyze')

        upsert_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=self.get_name('upsert'),
            input_node=None,
            output_node=None,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            command=const.SQL_RUNNER_COMMAND,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=depends_on
        )
        return upsert_pipeline_object

    @classmethod
    def get_table_from_def(cls, table_definition):
        with open(parse_path(table_definition)) as f:
            table_def_string = f.read()

        table = Table(SqlStatement(table_def_string))
        return table

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        step_args['pipeline_name'] = etl.name

        return step_args
