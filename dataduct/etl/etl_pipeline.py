"""
Class definition for DataPipeline
"""
import csv
import os
import yaml

from StringIO import StringIO
from copy import deepcopy
from datetime import datetime
from datetime import timedelta

from ..config import Config
from .utils import process_steps

from ..pipeline import DataPipeline
from ..pipeline import DefaultObject
from ..pipeline import Ec2Resource
from ..pipeline import EmrResource
from ..pipeline import RedshiftDatabase
from ..pipeline import S3Node
from ..pipeline import SNSAlarm
from ..pipeline import Schedule
from ..pipeline.utils import list_formatted_instance_details
from ..pipeline.utils import list_pipelines

from ..s3 import S3File
from ..s3 import S3LogPath
from ..s3 import S3Path

from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from ..utils.helpers import get_s3_base_path

import logging
logger = logging.getLogger(__name__)

config = Config()
S3_ETL_BUCKET = config.etl['S3_ETL_BUCKET']
MAX_RETRIES = config.etl.get('MAX_RETRIES', const.ZERO)
S3_BASE_PATH = config.etl.get('S3_BASE_PATH', const.EMPTY_STR)
SNS_TOPIC_ARN_FAILURE = config.etl.get('SNS_TOPIC_ARN_FAILURE', const.NONE)
NAME_PREFIX = config.etl.get('NAME_PREFIX', const.EMPTY_STR)
QA_LOG_PATH = config.etl.get('QA_LOG_PATH', const.QA_STR)
DP_INSTANCE_LOG_PATH = config.etl.get('DP_INSTANCE_LOG_PATH', const.NONE)
DP_PIPELINE_LOG_PATH = config.etl.get('DP_PIPELINE_LOG_PATH', const.NONE)

DEFAULT_TEARDOWN = {
    'step_type': 'transform',
    'command': 'echo Finished Pipeline',
    'no_output': True
}


class ETLPipeline(object):
    """DataPipeline class with steps and metadata.

    Datapipeline class contains all the metadata regarding the pipeline
    and has functionality to add steps to the pipeline

    """
    def __init__(self, name, frequency='one-time', ec2_resource_config=None,
                 time_delta=None, emr_cluster_config=None, load_time=None,
                 topic_arn=None, max_retries=MAX_RETRIES, teardown=None,
                 bootstrap=None, description=None):
        """Constructor for the pipeline class

        Args:
            name (str): Name of the pipeline should be globally unique.
            frequency (enum): Frequency of the pipeline. Can be
            time_delta(timedelta): Duration to change the start time by
            emr_cluster_config(dict): Dictionary for emr config
            topic_arn(str): sns alert to be used by the pipeline
            max_retries(int): number of retries for pipeline activities
            bootstrap(list of steps): bootstrap step definitions for resources
        """

        if load_time and isinstance(load_time, str):
            load_hour, load_min = [int(x) for x in load_time.split(':')]
        elif load_time and isinstance(load_time, int):
            load_hour, load_min = (load_time / 60, load_time % 60)
        else:
            load_hour, load_min = [None, None]

        if time_delta is None:
            time_delta = timedelta(seconds=0)

        # Input variables
        self._name = name if not NAME_PREFIX else NAME_PREFIX + '_' + name
        self.frequency = frequency
        self.load_hour = load_hour
        self.load_min = load_min
        self.time_delta = time_delta
        self.description = description
        self.max_retries = max_retries
        self.topic_arn = topic_arn

        if bootstrap is not None:
            self.bootstrap_definitions = bootstrap
        elif getattr(config, 'bootstrap', None):
            self.bootstrap_definitions = config.bootstrap
        else:
            self.bootstrap_definitions = dict()

        if teardown is not None:
            self.teardown_definition = teardown
        elif getattr(config, 'teardown', None):
            self.teardown_definition = config.teardown
        else:
            self.teardown_definition = DEFAULT_TEARDOWN

        if emr_cluster_config:
            self.emr_cluster_config = emr_cluster_config
        else:
            self.emr_cluster_config = dict()

        if ec2_resource_config:
            self.ec2_resource_config = ec2_resource_config
        else:
            self.ec2_resource_config = dict()

        # Pipeline versions
        self.version_ts = datetime.utcnow()
        self.version_name = "version_" + \
            self.version_ts.strftime('%Y%m%d%H%M%S')
        self.pipeline = None
        self.errors = None

        self._base_objects = dict()
        self.intermediate_nodes = dict()
        self._steps = dict()
        self._bootstrap_steps = list()

        # Base objects
        self.schedule = None
        self.sns = None
        self.default = None
        self._redshift_database = None
        self._ec2_resource = None
        self._emr_cluster = None
        self.create_base_objects()

    def __str__(self):
        """Formatted output when printing the pipeline object

        Returns:
            output(str): Formatted string output
        """
        output = ['%s : %s : %s' % (i, key, self._steps[key])
                  for i, key in enumerate(self._steps.keys())]
        return '\n'.join(output)

    def create_pipeline_object(self, object_class, **kwargs):
        """Abstract factory for creating, naming, and storing pipeline objects

        Args:
            object_class(PipelineObject): a class of pipeline objects
            **kwargs: keyword arguments to be passed to object class

        Returns:
            new_object(PipelineObject): Creates object based on class. Name of
            object is created on its type and index if not provided
        """
        instance_count = sum([1 for o in self._base_objects.values()
                              if isinstance(o, object_class)])

        # Object name/ids are given by [object_class][index]
        object_id = object_class.__name__ + str(instance_count)

        new_object = object_class(object_id, **kwargs)
        self._base_objects[object_id] = new_object
        return new_object

    def create_base_objects(self):
        """Create base pipeline objects

        Create base pipeline objects, which are maintained by ETLPipeline.
        The remaining objects (all of which are accessible through
        pipeline_objects) are maintained by the ETLStep.
        """

        # Base pipeline objects
        self.schedule = self.create_pipeline_object(
            object_class=Schedule,
            frequency=self.frequency,
            time_delta=self.time_delta,
            load_hour=self.load_hour,
            load_min=self.load_min,
        )
        if self.topic_arn is None and SNS_TOPIC_ARN_FAILURE is None:
            self.sns = None
        else:
            self.sns = self.create_pipeline_object(
                object_class=SNSAlarm,
                topic_arn=self.topic_arn,
                pipeline_name=self.name,
            )
        self.default = self.create_pipeline_object(
            object_class=DefaultObject,
            pipeline_log_uri=self.s3_log_dir,
        )

    @property
    def bootstrap_steps(self):
        """Get the bootstrap_steps for the pipeline

        Returns:
            result: bootstrap_steps for the pipeline
        """
        return self._bootstrap_steps

    @property
    def name(self):
        """Get the name of the pipeline

        Returns:
            result: name of the pipeline
        """
        return self._name

    @property
    def steps(self):
        """Get the steps of the pipeline

        Returns:
            result: steps of the pipeline
        """
        return self._steps

    def _s3_uri(self, data_type):
        """Get the S3 location for various data associated with the pipeline

        Args:
            data_type(enum of str): data whose s3 location needs to be fetched

        Returns:
            s3_path(S3Path): S3 location of directory of the given data type
        """
        if data_type not in [const.SRC_STR, const.LOG_STR, const.DATA_STR]:
            raise ETLInputError('Unknown data type found')

        # Versioning prevents using data from older versions
        key = [S3_BASE_PATH, data_type, self.name, self.version_name]

        if data_type == const.DATA_STR and self.frequency != 'one-time':
            # For repeated loads, include load date
            key.append("#{format(@scheduledStartTime, 'YYYYMMdd-HH-mm-ss')}")

        if data_type == const.LOG_STR:
            return S3LogPath(key, bucket=S3_ETL_BUCKET, is_directory=True)
        else:
            return S3Path(key, bucket=S3_ETL_BUCKET, is_directory=True)

    @property
    def s3_log_dir(self):
        """Fetch the S3 log directory

        Returns:
            s3_dir(S3Directory): Directory where s3 log will be stored.
        """
        return self._s3_uri(const.LOG_STR)

    @property
    def s3_data_dir(self):
        """Fetch the S3 data directory

        Returns:
            s3_dir(S3Directory): Directory where s3 data will be stored.
        """
        return self._s3_uri(const.DATA_STR)

    @property
    def s3_source_dir(self):
        """Fetch the S3 src directory

        Returns:
            s3_dir(S3Directory): Directory where s3 src will be stored.
        """
        return self._s3_uri(const.SRC_STR)

    @property
    def ec2_resource(self):
        """Get the ec2 resource associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns:
            ec2_resource(Ec2Resource): lazily-constructed ec2_resource
        """
        if not self._ec2_resource:
            self._ec2_resource = self.create_pipeline_object(
                object_class=Ec2Resource,
                s3_log_dir=self.s3_log_dir,
                schedule=self.schedule,
                **self.ec2_resource_config
            )
            self.create_bootstrap_steps(const.EC2_RESOURCE_STR)
        return self._ec2_resource

    @property
    def emr_cluster(self):
        """Get the emr resource associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns:
            emr_resource(EmrResource): lazily-constructed emr_resource
        """
        if not self._emr_cluster:
            # Process the boostrap input
            bootstrap = self.emr_cluster_config.get('bootstrap', None)
            overall_bootstrap = []
            if isinstance(bootstrap, dict):
                for key in bootstrap:
                    if 'string' in bootstrap:
                        overall_bootstrap.append(bootstrap[key])
                    elif 'script' in key:
                        # Set the S3 Path for the bootstrap script
                        bootstrap_file = S3File(path=bootstrap[key])
                        bootstrap_file.s3_path = self.s3_source_dir
                        overall_bootstrap.append(bootstrap_file)
            elif isinstance(bootstrap, str):
                # Set the S3 Path for the bootstrap script
                bootstrap = S3File(path=bootstrap)
                bootstrap.s3_path = self.s3_source_dir
                overall_bootstrap.append(bootstrap)
            else:
                overall_bootstrap = bootstrap
            self.emr_cluster_config['bootstrap'] = overall_bootstrap

            self._emr_cluster = self.create_pipeline_object(
                object_class=EmrResource,
                s3_log_dir=self.s3_log_dir,
                schedule=self.schedule,
                **self.emr_cluster_config
            )

            self.create_bootstrap_steps(const.EMR_CLUSTER_STR)
        return self._emr_cluster

    @property
    def redshift_database(self):
        """Get the redshift database associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns:
            redshift_database(Object): lazily-constructed redshift database
        """
        if not self._redshift_database:
            self._redshift_database = self.create_pipeline_object(
                object_class=RedshiftDatabase
            )
        return self._redshift_database

    def step(self, step_id):
        """Fetch a single step from the pipeline

        Args:
            step_id(str): id of the step to be fetched

        Returns:
            step(ETLStep): Step matching the step_id.
            If not found, None will be returned
        """
        return self._steps.get(step_id, None)

    def translate_input_nodes(self, input_node):
        """Translate names from YAML to input_nodes

        For steps which may take s3 as input, check whether they require
        multiple inputs. These inputs will be represented as a dictionary
        mapping step-names to filenames used in that step. E.g.

        ::

            {
                "step1": "eventing_activity_table",
                "step2": "activity_type_table"
            }

        When this is the case, we translate this to a dictionary in the
        following form, and pass that as the 'input_form':

        ::

            {
                "eventing_activity_table": [node for step1],
                "activity_type_table": [node for step2]
            }

        Args:
            input_node(dict): map of input node string

        Returns:
            output(dict of S3Node): map of string : S3Node
        """
        output = dict()
        for key, value in input_node.iteritems():
            if key not in self.intermediate_nodes:
                raise ETLInputError('Input reference does not exist')
            output[value] = self.intermediate_nodes[key]
        return output

    def add_step(self, step, is_bootstrap=False, is_teardown=False):
        """Add a step to the pipeline

        Args:
            step(ETLStep): Step object that should be added to the pipeline
            is_bootstrap(bool): flag indicating bootstrap steps
        """
        if step.id in self._steps:
            raise ETLInputError('Step name %s already taken' % step.id)
        self._steps[step.id] = step

        if self.bootstrap_steps and not is_bootstrap and not is_teardown:
            step.add_required_steps(self.bootstrap_steps)

        if is_teardown:
            teardown_dependencies = deepcopy(self._steps)
            teardown_dependencies.pop(step.id)
            step.add_required_steps(teardown_dependencies.values())

        # Update intermediate_nodes dict
        if isinstance(step.output, dict):
            self.intermediate_nodes.update(step.output)
        elif step.output and step.id:
            self.intermediate_nodes[step.id] = step.output

    def create_steps(self, steps_params, is_bootstrap=False,
                     is_teardown=False):
        """Create pipeline steps and add appropriate dependencies

        Note:
            Unless the input of a particular step is specified, it is assumed
            that its input is the preceding step.

        Args:
            steps_params(list of dict): List of dictionary of step params
            is_bootstrap(bool): flag indicating bootstrap steps
            is_teardown(bool): flag indicating teardown steps

        Returns:
            steps(list of ETLStep): list of etl step objects
        """
        input_node = None
        steps = []
        steps_params = process_steps(steps_params)
        for step_param in steps_params:

            # Assume that the preceding step is the input if not specified
            if isinstance(input_node, S3Node) and \
                    'input_node' not in step_param and \
                    'input_path' not in step_param:
                step_param['input_node'] = input_node

            if is_teardown:
                step_param['sns_object'] = self.sns

            try:
                step_class = step_param.pop('step_class')
                step_args = step_class.arguments_processor(self, step_param)
            except Exception:
                logger.error('Error creating step with params: %s', step_param)
                raise

            try:
                step = step_class(**step_args)
            except Exception:
                logger.error('Error creating step of class %s, step_param %s',
                             str(step_class.__name__), str(step_args))
                raise

            # Add the step to the pipeline
            self.add_step(step, is_bootstrap, is_teardown)
            input_node = step.output
            steps.append(step)
        return steps

    def create_teardown_step(self):
        """Create teardown steps for the pipeline
        """
        return self.create_steps([self.teardown_definition], is_teardown=True)

    def create_bootstrap_steps(self, resource_type):
        """Create the boostrap steps for installation on all machines

        Args:
            resource_type(enum of str): type of resource we're bootstraping
                can be ec2 / emr
        """
        step_params = self.bootstrap_definitions.get(resource_type, list())
        steps = self.create_steps(step_params, True)
        self._bootstrap_steps.extend(steps)
        return steps

    def pipeline_objects(self):
        """Get all pipeline objects associated with the ETL

        Returns:
            result(list of PipelineObject): All steps related to the ETL
            i.e. all base objects as well as ones owned by steps
        """
        result = self._base_objects.values()
        # Add all steps owned by the ETL steps
        for step in self._steps.values():
            result.extend(step.pipeline_objects)
        return result

    @staticmethod
    def log_uploader(uri, filename, string):
        """Utility function to upload log files to S3
        """
        dp_dir = S3Path(uri=uri, is_directory=True)
        dp_path = S3Path(
            key=filename + '.tsv',
            parent_dir=dp_dir,
        )
        dp_file = S3File(
            text=string,
            s3_path=dp_path,
        )
        dp_file.upload_to_s3()

    def log_s3_dp_instance_data(self, pipeline):
        """Uploads instance info for dp_instances to S3
        """
        dp_instance_entries = list_formatted_instance_details(pipeline)
        if len(dp_instance_entries) > 0:

            output_string = StringIO()
            writer = csv.writer(output_string, delimiter='\t')
            writer.writerows(dp_instance_entries)

            # S3 Path computation
            uri = os.path.join(get_s3_base_path(), QA_LOG_PATH,
                               DP_INSTANCE_LOG_PATH,
                               datetime.utcnow().strftime('%Y%m%d'))

            self.log_uploader(uri, pipeline.id, output_string.getvalue())
            output_string.close()

    def log_s3_dp_pipeline_data(self):
        """Uploads instance info for dp_pipeline to S3
        """
        output_string = StringIO()
        writer = csv.writer(output_string, delimiter='\t')
        writer.writerow([
            self.pipeline.id,
            self.name,
            self.version_ts
        ])

        # S3 Path computation
        uri = os.path.join(get_s3_base_path(), QA_LOG_PATH,
                           DP_PIPELINE_LOG_PATH,
                           datetime.utcnow().strftime('%Y%m%d'))

        self.log_uploader(uri, self.pipeline.id, output_string.getvalue())
        output_string.close()

    def delete_if_exists(self):
        """Delete the pipelines with the same name as current pipeline
        """

        # This will delete all pipelines with the same name
        for p_iter in list_pipelines():
            if p_iter['name'] == self.name:
                pipeline_instance = DataPipeline(pipeline_id=p_iter['id'])

                if DP_INSTANCE_LOG_PATH:
                    self.log_s3_dp_instance_data(pipeline_instance)
                pipeline_instance.delete()

    def s3_files(self):
        """Get all s3 files associated with the ETL

        Returns:
            result(list of s3files): All s3files related to the ETL
        """
        result = list()
        for pipeline_object in self.pipeline_objects():
            result.extend(pipeline_object.s3_files)
        return result

    def get_tags(self):
        """Get all the pipeline tags that are specified in the config
        """
        tag_config = config.etl.get('TAGS', None)
        if tag_config is None:
            return None

        tags = []
        for key, value in tag_config.iteritems():
            if 'string' in value and 'variable' in value:
                raise ETLInputError(
                    'Tag config can not have both string and variable')
            elif 'string' in value:
                tags.append({'key': key, 'value': value['string']})
            elif 'variable' in value:
                variable = getattr(self, value['variable'])
                tags.append({'key': key, 'value': variable})
        return tags

    def validate(self):
        """Validate the given pipeline definition by creating a pipeline

        Returns:
            errors(list): list of errors in the pipeline, empty if no errors
        """
        # Create AwsPipeline and add objects to it
        self.pipeline = DataPipeline(unique_id=self.name,
                                     description=self.description,
                                     tags=self.get_tags())

        for pipeline_object in self.pipeline_objects():
            self.pipeline.add_object(pipeline_object)

        # Check for errors
        self.errors = self.pipeline.validate_pipeline_definition()
        if len(self.errors) > 0:
            logger.error('There are errors with your pipeline:\n %s',
                         self.errors)

        # Update pipeline definition
        self.pipeline.update_pipeline_definition()
        return self.errors

    def activate(self):
        """Activate the given pipeline definition

        Activates an existing data pipeline & uploads all required files to s3
        """

        if self.errors is None:
            raise ETLInputError('Pipeline has not been validated yet')
        elif len(self.errors) > 0:
            raise ETLInputError('Pipeline has errors %s' % self.errors)

        # Upload any files that need to be uploaded
        for s3_file in self.s3_files():
            s3_file.upload_to_s3()

        # Upload pipeline definition
        pipeline_definition_path = S3Path(
            key='pipeline_definition.yaml',
            parent_dir=self.s3_source_dir
        )

        pipeline_definition = S3File(
            text=yaml.dump(self.pipeline.aws_format),
            s3_path=pipeline_definition_path
        )
        pipeline_definition.upload_to_s3()

        # Upload pipeline instance metadata to S3
        if DP_PIPELINE_LOG_PATH:
            self.log_s3_dp_pipeline_data()

        # Activate the pipeline with AWS
        self.pipeline.activate()
