"""
Class definition for DataPipeline
"""
from datetime import datetime
import yaml

from .constants import DEFAULT_MAX_RETRIES
from .constants import ETL_BUCKET
from .constants import BOOTSTRAP_STEPS_DEFINITION

from .pipeline.default_object import DefaultObject
from .pipeline.data_pipeline import DataPipeline
from .pipeline.ec2_resource import Ec2Resource
from .pipeline.emr_resource import EmrResource
from .pipeline.redshift_database import RedshiftDatabase
from .pipeline.s3_node import S3Node
from .pipeline.schedule import Schedule
from .pipeline.sns_alarm import SNSAlarm
from .pipeline.utils import list_pipelines

from .steps.emr_streaming import EMRStreamingStep
from .steps.extract_local import ExtractLocalStep
from .steps.extract_rds import ExtractRdsStep
from .steps.extract_redshift import ExtractRedshiftStep
from .steps.extract_s3 import ExtractS3Step
from .steps.load_redshift import LoadRedshiftStep
from .steps.sql_command import SqlCommandStep
from .steps.transform import TransformStep

from .s3.s3_file import S3File
from .s3.s3_path import S3Path
from .s3.s3_log_path import S3LogPath

from .utils.exceptions import ETLInputError


EC2_RESOURCE_STR = 'ec2'
EMR_CLUSTER_STR = 'emr'
LOG_STR = 'logs'
DATA_STR = 'data'
SRC_STR = 'src'


class ETLPipeline(object):
    """DataPipeline class with steps and metadata.

    Datapipeline class contains all the metadata regarding the pipeline
    and has functionality to add steps to the pipeline

    """
    def __init__(self, name, frequency='one-time',
                 ec2_resource_terminate_after='6 Hours',
                 delay=None, emr_cluster_config=None, load_time=None,
                 max_retries=DEFAULT_MAX_RETRIES):
        """Example of docstring on the __init__ method.

        The __init__ method may be documented in either the class level
        docstring, or as a docstring on the __init__ method itself.

        Either form is acceptable, but the two should not be mixed. Choose one
        convention to document the __init__ method and be consistent with it.

        Note:
            Do not include the `self` parameter in the ``Args`` section.

        Args:
            name (str): Name of the pipeline should be globally unique.
            frequency (enum): Frequency of the pipeline. Can be
            attr2 (list of str): Description of `attr2`.
            attr3 (int): Description of `attr3`.

        """
        if load_time:
            load_hour, load_min = [int(x) for x in load_time.split(':')]
        else:
            load_hour, load_min = [None, None]

        # Input variables
        self._name = name
        self.frequency = frequency
        self.ec2_resource_terminate_after = ec2_resource_terminate_after
        self.delay = delay
        self.load_hour = load_hour
        self.load_min = load_min
        self.max_retries = max_retries

        if emr_cluster_config:
            self.emr_cluster_config = emr_cluster_config
        else:
            self.emr_cluster_config = dict()

        # Pipeline versions
        self.version_ts = datetime.utcnow()
        self.version_name = "version_" + \
            self.version_ts.strftime('%Y%m%d%H%M%S')
        self.pipeline = None
        self.errors = None

        self._base_objects = dict()
        self._intermediate_nodes = dict()
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
        output = ['%s : %s : %s' % (i, key, self._steps[key]) \
                  for i, key in enumerate(self._steps.keys())]
        return '\n'.join(output)

    def create_pipeline_object(self, object_class, **kwargs):
        """Abstract factory for creating, naming, and storing pipeline objects

        Args:
            object_class(PipelineObject): a class of pipeline objects
            **kwargs: keyword arguments to be passed to object class

        Returns:
            new_object(PipelineObject): Creates object based on class.
                Name of object is created on its type and index if not provided
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
            delay=self.delay,
            load_hour=self.load_hour,
            load_min=self.load_min,
        )
        # self.sns = None -> Used for testing
        self.sns = self.create_pipeline_object(
            object_class=SNSAlarm,
            pipeline_name=self.name
        )
        self.default = self.create_pipeline_object(
            object_class=DefaultObject,
            sns=self.sns,
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

    def _s3_uri(self, data_type):
        """Get the S3 location for various data associated with the pipeline
        Args:
            data_type(enum of str): data whose s3 location needs to be fetched
        Returns:
            s3_path(S3Path): S3 location of directory of the given data type
        """
        if data_type not in [SRC_STR, LOG_STR, DATA_STR]:
            raise ETLInputError('Unknown data type found')

        # Versioning prevents using data from older versions
        key = [data_type, self.name, self.version_name]

        if self.frequency == 'daily' and data_type in [LOG_STR, DATA_STR]:
            # For repeated loads, include load date
            key.append("#{format(@scheduledStartTime, 'YYYYMMdd')}")

        if data_type == LOG_STR:
            return S3LogPath(key, bucket=ETL_BUCKET, is_directory=True)
        else:
            return S3Path(key, bucket=ETL_BUCKET, is_directory=True)

    @property
    def s3_log_dir(self):
        """Fetch the S3 log directory
        Returns:
            s3_dir(S3Directory): Directory where s3 log will be stored.
        """
        return self._s3_uri(LOG_STR)

    @property
    def s3_data_dir(self):
        """Fetch the S3 data directory
        Returns:
            s3_dir(S3Directory): Directory where s3 data will be stored.
        """
        return self._s3_uri(DATA_STR)

    @property
    def s3_source_dir(self):
        """Fetch the S3 src directory
        Returns:
            s3_dir(S3Directory): Directory where s3 src will be stored.
        """
        return self._s3_uri(SRC_STR)

    @property
    def ec2_resource(self):
        """Get the ec2 resource associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns: lazily-constructed ec2_resource
        """
        if not self._ec2_resource:
            self._ec2_resource = self.create_pipeline_object(
                object_class=Ec2Resource,
                s3_log_dir=self.s3_log_dir,
                schedule=self.schedule,
                terminate_after=self.ec2_resource_terminate_after,
            )

            self.create_bootstrap_steps(EC2_RESOURCE_STR)
        return self._ec2_resource

    @property
    def emr_cluster(self):
        """Get the emr resource associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns: lazily-constructed emr_resource
        """
        if not self._emr_cluster:
            # Process the boostrap input
            bootstrap = self.emr_cluster_config.get('bootstrap', None)
            if bootstrap:
                if isinstance(bootstrap, dict):
                    # If bootstrap script is not a path to local file
                    param_type = bootstrap['type']
                    bootstrap = bootstrap['value']
                else:
                    # Default the type to path of a local file
                    param_type = 'path'

                if param_type == 'path':
                    bootstrap = S3File(path=bootstrap)
                    # Set the S3 Path for the bootstrap script
                    bootstrap.s3_path = self.s3_source_dir
                self.emr_cluster_config['bootstrap'] = bootstrap

            self._emr_cluster = self.create_pipeline_object(
                object_class=EmrResource,
                s3_log_dir=self.s3_log_dir,
                schedule=self.schedule,
                **self.emr_cluster_config
            )

            self.create_bootstrap_steps(EMR_CLUSTER_STR)
        return self._emr_cluster

    @property
    def redshift_database(self):
        """Get the redshift database associated with the pipeline

        Note:
            This will create the object if it doesn't exist

        Returns: lazily-constructed redshift database object
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

    def determine_step_class(self, type, step_args):
        """Determine step class from input to correct ETL step types

        Args:
            type(str): string specifing type of the objects
            step_args(dict): dictionary of step arguments

        Returns:
            step_class(ETLStep): Class object for the specific type
            step_args(dict): dictionary of step arguments
        """
        if type == 'transform':
            step_class = TransformStep
            if step_args.get('resource', None) == 'emr-cluster':
                step_args['resource'] = self.emr_cluster

        elif type == 'extract-s3':
            step_class = ExtractS3Step
            step_args.pop('resource')

        elif type == 'extract-local':
            step_class = ExtractLocalStep
            step_args.pop('resource')
            if self.frequency != 'one-time':
                raise ETLInputError(
                    'Extract Local can be used for one-time pipelines only')

        elif type == 'extract-rds':
            step_class = ExtractRdsStep
            step_args.pop('input_node', None)

        elif type == 'extract-redshift':
            step_class = ExtractRedshiftStep
            step_args['redshift_database'] = self.redshift_database
            step_args.pop('input_node', None)

        elif type == 'sql-command':
            step_class = SqlCommandStep
            step_args['redshift_database'] = self.redshift_database
            step_args.pop('input_node', None)

        elif type == 'emr-streaming':
            step_class = EMRStreamingStep
            step_args['resource'] = self.emr_cluster

        elif type == 'load-redshift':
            step_class = LoadRedshiftStep
            step_args['redshift_database'] = self.redshift_database

        else:
            raise ETLInputError('Step type %s not recogonized' % type)

        return step_class, step_args

    def translate_input_nodes(self, input_node):
        """Translate names from YAML to input_nodes

        For steps which may take s3 as input, check whether they require
        multiple inputs. These inputs will be represented as a dictionary
        mapping step-names to filenames used in that step. E.g.
            {
                "step1": "eventing_activity_table",
                "step2": "activity_type_table"
            }

        When this is the case, we translate this to a dictionary in the
        following form, and pass that as the 'input_form':
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
            if key not in self._intermediate_nodes:
                raise ETLInputError('Input reference does not exist')
            output[value] = self._intermediate_nodes[key]
        return output

    def parse_step_args(self, type, **kwargs):
        """Parse step arguments from input to correct ETL step types

        Args:
            type(str): string specifing type of the objects
            **kwargs: Keyword arguments read from YAML

        Returns:
            step_class(ETLStep): Class object for the specific type
            step_args(dict): dictionary of step arguments
        """

        if not isinstance(type, str):
            raise ETLInputError('Step type must be a string')

        # Base dictionary for every step
        step_args = {
            'resource': None,
            'schedule': self.schedule,
            'max_retries': self.max_retries,
            'required_steps': list()
        }
        step_args.update(kwargs)

        # Description is optional and should not be passed
        step_args.pop('description', None)

        # Add dependencies
        depends_on = step_args.pop('depends_on', None)
        if depends_on:
            for step_id in list(depends_on):
                if step_id not in self._steps:
                    raise ETLInputError('Step depends on non-existent step')
                step_args['required_steps'].append(self._steps[step_id])

        step_class, step_args = self.determine_step_class(type, step_args)

        # Set input node and required_steps
        input_node = step_args.get('input_node', None)
        if input_node:
            if isinstance(input_node, dict):
                input_node = self.translate_input_nodes(input_node)
            elif isinstance(input_node, str):
                input_node = self._intermediate_nodes[input_node]
            step_args['input_node'] = input_node

            # Add dependencies from steps that create input nodes
            if isinstance(input_node, dict):
                required_nodes = input_node.values()
            else:
                required_nodes = [input_node]

            for required_node in required_nodes:
                for step in self._steps.values():
                    if step not in step_args['required_steps'] and \
                            required_node in step.pipeline_objects:
                        step_args['required_steps'].append(step)

        # Set resource for the step if not specified or removed
        if 'resource' in step_args and step_args['resource'] is None:
            step_args['resource'] = self.ec2_resource

        # Set the name if name not provided
        if 'name' in step_args:
            name = step_args.pop('name')
        else:
            # If the name of the step is not provided, one is assigned as:
            #   [step_class][index]
            name = step_class.__name__ + str(sum(
                [1 for a in self._steps.values() if isinstance(a, step_class)]
            ))

        # Each step is given it's own directory so that there is no clashing
        # of file names.
        step_args.update({
            'id': name,
            's3_log_dir': S3LogPath(name, parent_dir=self.s3_log_dir,
                                    is_directory=True),
            's3_data_dir': S3Path(name, parent_dir=self.s3_data_dir,
                                  is_directory=True),
            's3_source_dir': S3Path(name, parent_dir=self.s3_source_dir,
                                    is_directory=True),
        })

        return step_class, step_args

    def add_step(self, step, is_bootstrap=False):
        """Add a step to the pipeline
        Args:
            step(ETLStep): Step object that should be added to the pipeline
            is_bootstrap(bool): flag indicating bootstrap steps
        """
        if step.id in self._steps:
            raise ETLInputError('Step name %s already taken' % step.id)
        self._steps[step.id] = step

        if self.bootstrap_steps and not is_bootstrap:
            step.add_required_steps(self.bootstrap_steps)

        # Update intermediate_nodes dict
        if isinstance(step.output, dict):
            self._intermediate_nodes.update(step.output)
        elif step.output and step.id:
            self._intermediate_nodes[step.id] = step.output

    def create_steps(self, steps_params, is_bootstrap=False):
        """Create pipeline steps and add appropriate dependencies

        Note:
            Unless the input of a particular step is specified, it is assumed
            that its input is the preceding step.

        Args:
            steps_params(list of dict): List of dictionary of step params
            is_bootstrap(bool): flag indicating bootstrap steps
        Returns:
            steps(list of ETLStep): list of etl step objects
        """
        input_node = None
        steps = []
        for step_param in steps_params:

            # Assume that the preceding step is the input if not specified
            if isinstance(input_node, S3Node) and \
                    'input_node' not in step_param:
                step_param['input_node'] = input_node

            step_class, step_args = self.parse_step_args(**step_param)

            try:
                step = step_class(**step_args)
            except Exception:
                print "Error creating step of class %s, step_param %s." % (
                    str(step_class.__name__),
                    str(step_args)
                )
                raise

            # Add the step to the pipeline
            self.add_step(step, is_bootstrap)
            input_node = step.output
            steps.append(step)
        return steps

    def create_bootstrap_steps(self, resource_type):
        """Create the boostrap steps for installation on all machines

        Args:
            resource_type(enum of str): type of resource we're bootstraping
                can be ec2 / emr
        """
        if resource_type == EMR_CLUSTER_STR:
            resource = self.emr_cluster
        elif resource_type == EC2_RESOURCE_STR:
            resource = self.ec2_resource
        else:
            raise ETLInputError('Unknown resource type found')

        step_params = BOOTSTRAP_STEPS_DEFINITION
        for step in step_params:
            step['name'] += '_' + resource_type  # Append type for unique names
            if 'resource' in step:
                step['resource'] = resource

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

    def delete_if_exists(self):
        """Delete the pipelines with the same name as current pipeline
        """

        # This will delete all pipelines with the same name
        for p_iter in list_pipelines():
            if p_iter['name'] == self.name:
                pipeline_instance = DataPipeline(pipeline_id=p_iter['id'])
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

    def validate(self):
        """Validate the given pipeline definition by creating a pipeline

        Returns:
            errors(list): list of errors in the pipeline, empty if no errors
        """

        # Create AwsPipeline and add objects to it
        self.pipeline = DataPipeline(self.name)
        for pipeline_object in self.pipeline_objects():
            self.pipeline.add_object(pipeline_object)

        # Check for errors
        self.errors = self.pipeline.validate_pipeline_definition()
        if len(self.errors) > 0:
            print '\nThere are errors with your pipeline:\n', self.errors

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

        # Activate the pipeline with AWS
        self.pipeline.activate()
