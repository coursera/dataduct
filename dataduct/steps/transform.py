"""
ETL step wrapper for shell command activity can be executed on Ec2 / EMR
"""
import os

from .etl_step import ETLStep
from ..pipeline import ShellCommandActivity
from ..pipeline import S3Node
from ..s3 import S3File
from ..s3 import S3Directory
from ..utils.helpers import exactly_one
from ..utils.helpers import get_modified_s3_path
from ..utils.exceptions import ETLInputError
from ..utils import constants as const

import logging
logger = logging.getLogger(__name__)

SCRIPT_ARGUMENT_TYPE_STRING = 'string'
SCRIPT_ARGUMENT_TYPE_SQL = 'sql'


class TransformStep(ETLStep):
    """Transform Step class that helps run scripts on resources
    """

    def __init__(self,
                 command=None,
                 script=None,
                 script_directory=None,
                 script_name=None,
                 script_arguments=None,
                 additional_s3_files=None,
                 output_node=None,
                 output_path=None,
                 no_output=False,
                 no_input=False,
                 **kwargs):
        """Constructor for the TransformStep class

        Args:
            command(str): command to be executed directly
            script(path): local path to the script that should executed
            script_directory(path): local path to the script directory
            script_name(str): script to be executed in the directory
            script_arguments(list of str): list of arguments to the script
            additional_s3_files(list of S3File): additional files used
            output_node(dict): output data nodes from the transform
            output_path(str): the S3 path to output data
            no_output(bool): whether we output anything at all.
            no_input(bool): whether the script takes any inputs
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(TransformStep, self).__init__(**kwargs)

        if not exactly_one(command, script, script_directory):
            raise ETLInputError(
                'Only one of script, command and directory allowed')

        base_output_node = None
        if not no_output:
            # Create output_node based on output_path
            base_output_node = self.create_s3_data_node(
                self.get_output_s3_path(get_modified_s3_path(output_path)))

        script_arguments = self.translate_arguments(script_arguments)
        if script_arguments is None:
            script_arguments = []

        if self.input and not no_input:
            input_nodes = [self.input]
        else:
            input_nodes = []

        if script_directory:
            # The script to be run with the directory
            if script_name is None:
                raise ETLInputError('script_name required with directory')

            script_directory = self.create_script(
                S3Directory(path=script_directory))

            # Input node for the source code in the directory
            input_nodes.append(self.create_pipeline_object(
                object_class=S3Node,
                schedule=self.schedule,
                s3_object=script_directory
            ))

            # We need to create an additional script that later calls the main
            # script as we need to change permissions of the input directory
            ip_src_env = 'INPUT%d_STAGING_DIR' % (1 if not self.input else 2)
            additional_args = ['--INPUT_SRC_ENV_VAR=%s' % ip_src_env,
                               '--SCRIPT_NAME=%s' % script_name]

            script_arguments = additional_args + script_arguments

            steps_path = os.path.abspath(os.path.dirname(__file__))
            script = os.path.join(steps_path, const.SCRIPT_RUNNER_PATH)

        # Create S3File if script path provided
        if script:
            script = self.create_script(S3File(path=script))

        # Translate output nodes if output path provided
        if output_node:
            self._output = self.create_output_nodes(
                base_output_node, output_node)
        else:
            self._output = base_output_node

        logger.debug('Script Arguments:')
        logger.debug(script_arguments)

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=input_nodes,
            output_node=base_output_node,
            resource=self.resource,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            command=command,
            max_retries=self.max_retries,
            depends_on=self.depends_on,
            additional_s3_files=additional_s3_files,
        )

    def translate_arguments(self, script_arguments):
        """Translate script argument to lists

        Args:
            script_arguments(list of str/dict): arguments to the script

        Note:
            Dict: (k -> v) is turned into an argument "--k=v"
            List: Either pure strings or dictionaries with name, type and value
        """
        if script_arguments is None:
            return script_arguments

        elif isinstance(script_arguments, list):
            result = list()
            for argument in script_arguments:
                if isinstance(argument, dict):
                    result.extend([
                        self.input_format(key, get_modified_s3_path(value))
                        for key, value in argument.iteritems()
                    ])
                else:
                    result.append(get_modified_s3_path(str(argument)))
            return result

        elif isinstance(script_arguments, dict):
            return [self.input_format(key, get_modified_s3_path(value))
                    for key, value in script_arguments.iteritems()]

        elif isinstance(script_arguments, str):
            return [get_modified_s3_path(script_arguments)]

        else:
            raise ETLInputError('Script Arguments for unrecognized type')

    @staticmethod
    def input_format(key, value):
        """Format the key and value to command line arguments
        """
        return ''.join('--', key, '=', value)

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        if step_args.pop('resource_type', None) == const.EMR_CLUSTER_STR:
            step_args['resource'] = etl.emr_cluster
        else:
            step_args['resource'] = etl.ec2_resource

        return step_args
