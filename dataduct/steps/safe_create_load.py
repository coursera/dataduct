"""ETL step wrapper for loading into redshift with the COPY command
"""
import os

from ..config import Config
from ..database import Table
from ..database import SqlStatement
from ..database import SqlScript
from .etl_step import ETLStep
from ..pipeline import ShellCommandActivity
from ..s3 import S3File
from ..utils import constants as const
from ..utils.helpers import parse_path

config = Config()


class SafeCreateAndLoadStep(ETLStep):
    """SafeCreateAndLoadStep Step class that creates table if needed and loads data
    """

    def __init__(self, id, input_node, intermediate_table_definition,
                 table_definition, pipeline_name, script_arguments=None,
                 analyze_table=True, non_transactional=False,
                 log_to_s3=False, **kwargs):
        """Constructor for the SafeCreateAndLoadStep class

        Args:
            input_node: A S3 data node as input
            intermediate_table_definition(filepath):
                intermediate table schema to store the data
            table_definition(filepath):
                schema file for the table to be reloaded into
            script_arguments(list of str): list of arguments to the script
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        super(SafeCreateAndLoadStep, self).__init__(id=id, **kwargs)
        if script_arguments is None:
            script_arguments = list()

        create_and_load_pipeline_object = self.create_and_load_redshift(
            table_definition=intermediate_table_definition,
            input_node=input_node,
            script_arguments=script_arguments
        )

        reload_pipeline_object = self.reload(
            source=intermediate_table_definition,
            destination=table_definition,
            depends_on=[create_and_load_pipeline_object],
            analyze_table=analyze_table,
            non_transactional=non_transactional,
            script_arguments=script_arguments
        )

        primary_key_check_object = self.primary_key_check(
            table_definition=table_definition,
            pipeline_name=pipeline_name,
            depends_on=[reload_pipeline_object],
            script_arguments=script_arguments,
            log_to_s3=log_to_s3
        )


    def primary_key_check(self, table_definition, pipeline_name, depends_on,
                          script_arguments, log_to_s3):

        table = self.get_table_from_def(table_definition)

        # We initialize the table object to check valid strings
        script_arguments.append('--table=%s' % table.sql())

        if log_to_s3:
            script_arguments.append('--log_to_s3')

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.PK_CHECK_SCRIPT_PATH)
        script = self.create_script(S3File(path=script))

        step_name = self.get_name("primary_key_check")
        script_arguments.append('--test_name=%s' % (pipeline_name + "." + step_name))

        sns_topic_arn = config.etl.get('SNS_TOPIC_ARN_WARNING', None)
        if sns_topic_arn:
            script_arguments.append('--sns_topic_arn=%s' % sns_topic_arn)

        primary_key_check_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=step_name,
            input_node=[],
            output_node=None,
            resource=self.resource,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=depends_on
        )

        return primary_key_check_pipeline_object


    def reload(self, source, destination, depends_on, script_arguments,
               analyze_table, non_transactional):
        source_table = parse_path(source)
        destination_table = parse_path(destination)

        source_relation = Table(SqlScript(filename=source_table))
        destination_relation = Table(SqlScript(filename=destination_table))

        # Reload specific config
        enforce_primary_key = False
        delete_existing = True
        sql_script = destination_relation.upsert_script(
            source_relation, enforce_primary_key, delete_existing)

        update_script = SqlScript(sql_script.sql())
        script_arguments.extend([
            '--table_definition=%s' % destination_relation.sql(),
            '--sql=%s' % update_script.sql()
        ])

        if analyze_table:
            script_arguments.append('--analyze')

        if non_transactional:
            script_arguments.append('--non_transactional')

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.SQL_RUNNER_SCRIPT_PATH)
        script = self.create_script(S3File(path=script))

        reload_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=self.get_name("reload"),
            input_node=[],
            output_node=None,
            resource=self.resource,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=depends_on
        )

        return reload_pipeline_object


    def create_and_load_redshift(self, table_definition, input_node,
                                 script_arguments):
        table = self.get_table_from_def(table_definition)

        if isinstance(input_node, dict):
            input_paths = [i.path().uri for i in input_node.values()]
        else:
            input_paths = [input_node.path().uri]

        script_arguments.extend([
            '--table_definition=%s' % table.sql().sql(),
            '--s3_input_paths'] + input_paths)

        steps_path = os.path.abspath(os.path.dirname(__file__))
        script = os.path.join(steps_path, const.CREATE_LOAD_SCRIPT_PATH)
        script = self.create_script(S3File(path=script))

        # Create output_node based on output_path
        base_output_node = self.create_s3_data_node()

        create_and_load_pipeline_object = self.create_pipeline_object(
            object_class=ShellCommandActivity,
            object_name=self.get_name("create_and_load"),
            input_node=[input_node],
            output_node=base_output_node,
            resource=self.resource,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            max_retries=self.max_retries,
            depends_on=self.depends_on
        )

        return create_and_load_pipeline_object


    def get_table_from_def(self, table_definition):
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
        step_args['resource'] = etl.ec2_resource
        step_args['pipeline_name'] = etl.name
        return step_args
