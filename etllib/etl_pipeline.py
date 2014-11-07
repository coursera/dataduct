"""
Class definition for DataPipeline
"""
from datetime import datetime

from .constants import DEFAULT_MAX_RETRIES
from .pipeline.default_object import DefaultObject
from .pipeline.ec2_resource import Ec2Resource
from .pipeline.schedule import Schedule
from .pipeline.sns_alarm import SNSAlarm




class ETLPipeline(object):
    """DataPipeline class with steps and metadata.

    Datapipeline class contains all the metadata regarding the pipeline
    and has functionality to add steps to the pipeline

    """
    def __init__(self, name, frequency='one-time',
                 ec2_resource_terminate_after='24 Hours',
                 delay=None, emr_cluster_config=None, load_min=None,
                 load_hour=None, max_retries=DEFAULT_MAX_RETRIES):
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

        self._base_objects = dict()
        self._intermediate_nodes = dict()
        self._steps = dict()
        self._bootstrap_steps = None


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

    def step(self, step_id):
        """Fetch a single step from the pipeline
        Args:
            step_id(str): id of the step to be fetched
        Returns:
            step(ETLStep): Step matching the step_id.
                If not found, None will be returned
        """
        return self._steps.get(step_id, None)


    def create_steps(self, param1):
        """Class methods are similar to regular functions.

        Note:
          Do not include the `self` parameter in the ``Args`` section.

        Args:
          param1: The first parameter.
          param2: The second parameter.

        Returns:
          True if successful, False otherwise.

        """
        return True
