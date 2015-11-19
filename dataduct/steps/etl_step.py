"""
Base class for an etl step
"""
from ..config import Config
from ..pipeline import Activity
from ..pipeline import CopyActivity
from ..pipeline import S3Node
from ..s3 import S3File
from ..s3 import S3LogPath
from ..s3 import S3Path
from ..utils import constants as const
from ..utils.exceptions import ETLInputError

config = Config()
MAX_RETRIES = config.etl.get('MAX_RETRIES', const.ZERO)


class ETLStep(object):
    """ETL step class with activities and metadata.

    An ETL Step is an abstraction over a unit of work. It
    represents a chunk of objects having the following attributes:

    -  input
    -  activities
    -  output

    Given the input parameters, the ETL Step is responsible for creating all
    necessary AWS pipeline objects, defining names for such objects, and
    defining s3 URI's for all dependent files.
    """

    def __init__(self, id, s3_data_dir=None, s3_log_dir=None,
                 s3_source_dir=None, schedule=None, resource=None,
                 worker_group=None, input_node=None, input_path=None,
                 required_steps=None, max_retries=MAX_RETRIES, sns_object=None):
        """Constructor for the ETLStep object

        Args:
            unique_id (str): Unique id for the pipeline
            name (str): Name of the pipeline (optional)
            pipeline_id (str): Id assigned by AWS and looks like df-xyz

        Note:
            If pipelineId is provided we don't need name or unique_id
        """

        # Initialize inputs
        self.id = id
        self.s3_log_dir = s3_log_dir
        self.s3_data_dir = s3_data_dir
        self.s3_source_dir = s3_source_dir
        self.schedule = schedule
        self.resource = resource
        self.worker_group = worker_group
        self.max_retries = max_retries
        self._depends_on = list()
        self._output = None
        self._objects = dict()
        self._required_steps = list()
        self._required_activities = list()
        self._input_node = input_node
        self._sns_object = sns_object

        if input_path is not None and input_node is not None:
            raise ETLInputError('Both input_path and input_node specified')

        if input_path is not None:
            self._input_node = self.create_s3_data_node(S3Path(uri=input_path))

        if isinstance(input_node, list):
            if len(input_node) == 0:
                input_node = None
            else:
                raise ETLInputError('Use dict format for input nodes')

        # Validate inputs
        if input_node is not None and not (isinstance(input_node, S3Node) or
                                           isinstance(input_node, dict)):
            raise ETLInputError('Input node must be S3Node')

        if isinstance(input_node, dict):
            # Merge the s3 nodes if there are multiple inputs
            self._input_node, self._depends_on = self.merge_s3_nodes(
                input_node)

        if required_steps:
            self.add_required_steps(required_steps)

    def __str__(self):
        """Output the ETL step when typecasted to string"""
        return 'ETL Step with id => %s' % self.id

    def add_required_steps(self, required_steps):
        """Add dependencies for this step

        Args:
            required_steps(list of ETLStep): dependencies of current step
        """
        self._required_steps.extend(required_steps)

        for step in required_steps:
            self._required_activities.extend(step.activities)

        # Set required_activities as the depend_on variable of all activities
        for activity in self.activities:
            activity['dependsOn'] = self._find_actual_required_activities()

    def _find_actual_required_activities(self):
        """Find the actual nodes that the activity should depend upon instead
            of all nodes that are sent as required steps
        """
        activity_set = set(self._required_activities)
        for required_activity in self._required_activities:
            activity_set -= self._resolve_dependencies(
                required_activity.depends_on)
        return list(activity_set)

    def _resolve_dependencies(self, dependencies):
        """Resolve the dependencies of the step recursively
        """
        dependencies = dependencies if isinstance(dependencies, list) else \
            [dependencies]
        result = set(dependencies)
        for dependency in dependencies:
            result |= self._resolve_dependencies(dependency.depends_on)
        return result

    def create_pipeline_object(self, object_class, object_name=None, **kwargs):
        """Create the pipeline objects associated with the step

        Args:
            object_class(PipelineObject): a class of pipeline objects
            **kwargs: keyword arguments to be passed to object class

        Returns:
            new_object(PipelineObject): Creates object based on class.
            Name of object is created on its type and index if not provided
        """
        instance_count = sum([1 for o in self._objects.values()
                              if isinstance(o, object_class)])

        # Object name/ids are given by [step_id].[object_class][index]
        if object_name is None:
            object_id = self.id + "." + object_class.__name__ + \
                str(instance_count)
        else:
            object_id = object_name

        new_object = object_class(object_id, **kwargs)

        if isinstance(new_object, Activity):
            new_object['dependsOn'] = self._required_activities

        if self._sns_object:
            new_object['onFail'] = self._sns_object

        self._objects[object_id] = new_object
        return new_object

    def create_s3_data_node(self, s3_object=None, object_name=None, **kwargs):
        """Create an S3 DataNode for s3_file or s3_path

        Args:
            s3_object(S3File / S3Path): s3_object to be used to create the node

        Returns:
            s3_node(S3Node): S3Node for the etl step
        """
        # Validate inputs
        if s3_object and not (isinstance(s3_object, S3File) or
                              isinstance(s3_object, S3Path)):
            raise ETLInputError('s3_object must of type S3File or S3Path')

        create_s3_path = False
        if s3_object is None or (isinstance(s3_object, S3File) and
                                 s3_object.s3_path is None):
            create_s3_path = True

        if create_s3_path:
            s3_dir = S3Path(parent_dir=self.s3_data_dir)
            if s3_object is None:
                s3_object = s3_dir

        s3_node = self.create_pipeline_object(
            object_class=S3Node,
            object_name=object_name,
            schedule=self.schedule,
            s3_object=s3_object,
            **kwargs
        )

        if create_s3_path:
            # Make the S3 path be the step directory plus s3 node name
            s3_dir.append(s3_node.id, is_directory=True)
            if isinstance(s3_object, S3File):
                # Put the file in the appropriate directory
                s3_object.s3_path = s3_dir

        return s3_node

    def create_output_nodes(self, output_node, sub_dirs):
        """Create output node for all the subdirs

        Args:
            output_node(dict of str) Base node from which the other
            directories are created
            sub_dirs(list of str): Name of the subdirectories

        Returns:
            s3_output_nodes(dict of s3Node): Output nodes keyed with sub dirs
        """
        output_map = dict()
        for sub_dir in sub_dirs:
            new_node = self.create_s3_data_node(
                S3Path(sub_dir, is_directory=True,
                       parent_dir=output_node.path()))
            new_node.add_dependency_node(output_node)

            output_map[sub_dir] = new_node

        return output_map

    def create_script(self, s3_object):
        """Set the s3 path for s3 objects with the s3_source_dir

        Args:
            s3_object(S3File): S3file for which the source directory is set

        Returns:
            s3_object(S3File): S3File after the path is set
        """
        s3_object.s3_path = self.s3_source_dir
        return s3_object

    def copy_s3(self, input_node, dest_uri):
        """Copy S3 node to new location in s3

        Creates a copy activity. Instead of copying to just the
        output_node, we create an intermediate node. This node must point to a
        directory, not a file. Otherwise, AWS will try to copy the file
        line by line.

        Args:
            input_node(S3Node): Source Node in S3
            dest_uri(S3Path): Destination path in S3

        Returns:
            activity(CopyActivity): copy activity object
        """
        if not(isinstance(input_node, S3Node) and
                isinstance(dest_uri, S3Path)):
            raise ETLInputError('input_node and uri have type mismatch')

        # create s3 node for output
        output_node = self.create_s3_data_node(dest_uri)

        # Create new input node if file and not directory
        if input_node.path().is_directory:
            new_input_node = input_node
        else:
            uri = "/".join(input_node.path().uri.split("/")[:-1])
            new_input_node = self.create_s3_data_node(
                S3Path(uri=uri, is_directory=True))
            new_input_node.add_dependency_node(input_node)

        # create copy activity
        activity = self.create_pipeline_object(
            CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
            worker_group=self.worker_group,
            input_node=new_input_node,
            output_node=output_node,
            max_retries=self.max_retries
        )

        return activity

    def merge_s3_nodes(self, input_nodes):
        """Merge multi S3 input nodes

        We merge the multiple input nodes by using a copy activity

        Args:
            input_nodes(dict of S3Node): Key-Node pair like {'node_name': node}

        Returns:
            combined_node(S3Node): New S3Node that has input nodes merged
            new_depends_on(list of str): new dependencies for the step
        """
        depends_on = list()
        combined_node = self.create_s3_data_node()

        for string_key, input_node in input_nodes.iteritems():
            dest_uri = S3Path(key=string_key, is_directory=True,
                              parent_dir=combined_node.path())
            copy_activity = self.copy_s3(input_node=input_node,
                                         dest_uri=dest_uri)
            depends_on.append(copy_activity)
            combined_node.add_dependency_node(copy_activity.output)

        return combined_node, depends_on

    def get_name(self, *suffixes):
        if all(suffixes):
            return '_'.join((self.id,) + suffixes)
        return None

    @property
    def input(self):
        """Get the input node for the etl step

        Returns:
            result: Input node for this etl step

        Note:
            Input is represented as None, a single node or dict of nodes
        """
        return self._input_node

    @property
    def output(self):
        """Get the output node for the etl step

        Returns:
            result: output node for this etl step

        Note:
            An output S3 node, or multiple s3 nodes. If step produces no s3
            nodes, there will be no output. For steps producing s3 output, note
            that they may produce multiple output nodes. These nodes will be
            defined in a list of output directories (specified in the
            load-definition) to the node. For instance, the step defined
            as follows:

            ::

                {
                    ...,
                    "output": [
                        "output1":,
                        "output2"
                    ]
                }

            will have output:

            ::

                {
                    "output1": [s3 node pointing to .../output_1]
                    "output2": [s3 node pointing to .../output_2]
                }
        """
        return self._output

    @property
    def depends_on(self):
        """Get the dependent activities for the etl step

        Returns:
            result: dependent activities for this etl step
        """
        return self._depends_on

    @property
    def maximum_retries(self):
        """Get the maximum retries for the etl step

        Returns:
            result: maximum retries for this etl step
        """
        return self.max_retries

    @property
    def pipeline_objects(self):
        """Get  all pipeline objects that are created for this step

        Returns:
            result: All pipeline objects that are created for this step
        """
        return self._objects.values()

    @property
    def activities(self):
        """Get  all aws activites that are created for this step

        Returns:
            result: All aws activites that are created for this step
        """
        return [x for x in self._objects.values() if isinstance(x, Activity)]

    @classmethod
    def base_arguments_processor(cls, etl, input_args,
                                 resource_type=const.EC2_RESOURCE_STR):
        """Process the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            input_args(dict): Dictionary of the step arguments from the YAML
            resource_type(str): either const.EMR_CLUSTER_STR
                or const.EC2_RESOURCE_STR
        """
        assert resource_type in [const.EMR_CLUSTER_STR,
                                  const.EC2_RESOURCE_STR],\
            'resource type must be one of %s or %s' %\
            (const.EC2_RESOURCE_STR, const.EMR_CLUSTER_STR)
        resource_or_worker_group = {}
        if resource_type == const.EMR_CLUSTER_STR:
            worker_group = config.emr.get('WORKER_GROUP', None)
            if worker_group:
                resource_or_worker_group['worker_group'] = worker_group
            else:
                resource_or_worker_group['resource'] = etl.emr_cluster
        else:
            worker_group = config.ec2.get('WORKER_GROUP', None)
            if worker_group:
                resource_or_worker_group['worker_group'] = worker_group
            else:
                resource_or_worker_group['resource'] = etl.ec2_resource

        # Base dictionary for every step
        step_args = {
            'schedule': etl.schedule,
            'max_retries': etl.max_retries,
            'required_steps': list(),
        }
        step_args.update(resource_or_worker_group)
        step_args.update(input_args)

        # Description is optional and should not be passed
        step_args.pop('description', None)

        # Add dependencies
        depends_on = step_args.pop('depends_on', None)
        if isinstance(depends_on, str):
            depends_on = [depends_on]

        if depends_on:
            for step_id in list(depends_on):
                if step_id not in etl.steps:
                    raise ETLInputError('Step depends on non-existent step')
                step_args['required_steps'].append(etl.steps[step_id])

        # Set input node and required_steps
        input_node = step_args.get('input_node', None)
        if input_node:
            if isinstance(input_node, dict):
                input_node = etl.translate_input_nodes(input_node)
            elif isinstance(input_node, str):
                input_node = etl.intermediate_nodes[input_node]
            step_args['input_node'] = input_node

            # Add dependencies from steps that create input nodes
            if isinstance(input_node, dict):
                required_nodes = input_node.values()
            else:
                required_nodes = [input_node]

            for required_node in required_nodes:
                for step in etl.steps.values():
                    if step not in step_args['required_steps'] and \
                            required_node in step.pipeline_objects:
                        step_args['required_steps'].append(step)

        # Set the name if name not provided
        if 'name' in step_args:
            name = step_args.pop('name')
        else:
            # If the name of the step is not provided, one is assigned as:
            #   [step_class][index]
            name = cls.__name__ + str(sum(
                [1 for a in etl.steps.values() if isinstance(a, cls)]
            ))

        # Each step is given it's own directory so that there is no clashing
        # of file names.
        step_args.update({
            'id': name,
            's3_log_dir': S3LogPath(name, parent_dir=etl.s3_log_dir,
                                    is_directory=True),
            's3_data_dir': S3Path(name, parent_dir=etl.s3_data_dir,
                                  is_directory=True),
            's3_source_dir': S3Path(name, parent_dir=etl.s3_source_dir,
                                    is_directory=True),
        })

        return step_args

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        step_args = cls.base_arguments_processor(etl, input_args)
        return step_args

    @staticmethod
    def pop_inputs(input_args):
        """Remove the input nodes from the arguments dictionary
        """
        input_args.pop('input_node', None)
        input_args.pop('input_path', None)

        return input_args

    @staticmethod
    def get_output_s3_path(output_path, is_directory=True):
        """Create an S3 Path variable based on the output path
        """
        if output_path:
            s3_path = S3Path(uri=output_path, is_directory=is_directory)
        else:
            s3_path = None
        return s3_path
