"""
Base class for an etl step
"""
import os

from ..constants import DEFAULT_MAX_RETRIES
from ..pipeline.activity import Activity
from ..pipeline.copy_activity import CopyActivity
from ..pipeline.s3_node import S3Node
from ..s3.s3_path import S3Path
from ..s3.s3_file import S3File
from ..utils.exceptions import ETLInputError


class ETLStep(object):
    """ETL step class with activities and metadata.

    An ETL Step is an abstraction over the set of each database object. It
    represents a chunk of objects having the following attributes:
        - input
        - activities
        - output
    Given the input parameters, the ETL Step is responsible for creating all
    necessary AWS pipeline objects, defining names for such objects, and
    defining s3 URI's for all dependent files.
    """

    def __init__(self, id, s3_data_dir=None, s3_log_dir=None,
                 s3_source_dir=None, schedule=None, resource=None,
                 input_node=None, required_steps=None,
                 max_retries=DEFAULT_MAX_RETRIES):
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
        self.max_retries = max_retries
        self._depends_on = list()
        self._input = input_node
        self._output = None
        self._objects = dict()

        self._activities = list()
        self._input_node = input_node

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
            self._input_node, self.depends_on = self.merge_s3_nodes(input_node)

        if required_steps:
            self._required_steps = self.add_required_steps(required_steps)
        else:
            self._required_steps = list()

    def __str__(self):
        """Output the ETL step when typecasted to string"""
        return 'ETL Step with id => %s' % self.id

    def add_required_steps(self, required_steps):
        """Add dependencies for this step

        Args:
            required_steps(list of ETLStep): dependencies of current step
        """

        self._required_steps.extend(required_steps)

        # Find all activities which need to be completed.
        required_activities = []
        for step in self._required_steps:
            required_activities.extend(step.activities)

        # Set required_acitivites as depend_on variable of all activities
        for activity in self.activities:
            activity['dependsOn'] = required_activities

    def create_pipeline_object(self, object_class, **kwargs):
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
        object_id = self.id + "." + object_class.__name__ + \
            str(instance_count)

        new_object = object_class(object_id, **kwargs)
        self._objects[object_id] = new_object
        return new_object

    def create_s3_data_node(self, s3_object=None, **kwargs):
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


        if s3_object is None or (isinstance(s3_object, S3File) and
                                 s3_object.s3_path is None):
            create_s3_path = True

        if create_s3_path:
            s3_dir = S3Path(parent_dir=self.s3_data_dir)
            if s3_object is None:
                s3_object = s3_dir

        s3_node = self.create_pipeline_object(
            S3Node,
            schedule=self.schedule,
            s3_path=s3_object,
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
        return dict(
            (
                sub_dir,
                self.create_s3_data_node(S3Path(sub_dir, is_directory=True,
                                                parent_dir=output_node.path()))
            ) for sub_dir in sub_dirs)

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
        if not(isinstance(input_node, S3Node) and isinstance(dest_uri, S3Path)):
            raise ETLInputError('input_node and uri have type mismatch')

        # Copy the input node. We need to use directories for copying if we
        # are going to omit the data format
        if input_node.path().is_directory:
            uri = input_node.path().uri
        else:
            base_uri = input_node.path().uri.split('/')[:-1]
            uri = os.path.join(*base_uri)

        new_input_node = self.create_s3_data_node(
            s3_object=S3Path(uri=uri, is_directory=True))

        # create s3 node for output
        output_node = self.create_s3_data_node(dest_uri)

        # create copy activity
        activity = self.create_pipeline_object(
            CopyActivity,
            schedule=self.schedule,
            resource=self.resource,
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
        for input_node in input_nodes:
            dest_uri = S3Path(key=input_node, is_directory=True,
                              parent_dir=combined_node.path())
            copy_activity = self.copy_s3(input_node=input_nodes[input_node],
                                         dest_uri=dest_uri)
            depends_on.append(copy_activity)
        return combined_node, depends_on

    @property
    def input(self):
        """Get the input node for the etl step

        Returns:
            result: Input node for this etl step

        Note:
            Input is represented as None, a single node or dict of nodes
        """
        return self._input

    @property
    def output(self):
        """Get the output node for the etl step

        Returns:
            result: output node for this etl step

        Note:
            An output S3 node, or multiple s3 nodes. If this step produces
            no s3 nodes, there will be no output.

            For steps producing s3 output, note that they may produce multiple
            output nodes. These nodes will be defined in a list of output
            directories (specified in the load_definition) to the node. For
            instance, the step defined as follows:
                {
                    ...,
                    "output": [
                        "output1":,
                        "output2"
                    ]
                }
            will have output:
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
