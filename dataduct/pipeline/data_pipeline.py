"""
Base class for data pipeline instance
"""
import json
from collections import defaultdict

from .pipeline_object import PipelineObject
from .utils import list_pipeline_instances
from .utils import get_datapipeline_connection
from ..utils.exceptions import ETLInputError


class DataPipeline(object):
    """DataPipeline classes with objects and metadata.

    The DataPipeline represents the data-pipeline, and is responsible
    for collecting all pipeline objects, validating the pipeline, and
    executing it.
    """

    def __init__(self, unique_id=None, name=None, pipeline_id=None,
                 tags=None, description=None):
        """Constructor for the datapipeline object

        Args:
            unique_id (str): Unique id for the pipeline
            name (str): Name of the pipeline (optional)
            pipeline_id (str): Id assigned by AWS and looks like df-xyz

        Note:
            If pipelineId is provided we don't need name or unique_id
        """
        self.conn = get_datapipeline_connection()
        self.objects = []

        if pipeline_id:
            if unique_id or name:
                raise ETLInputError('Cannot provide name with pipeline id')

            self.pipeline_id = pipeline_id
        else:
            if unique_id is None:
                raise ETLInputError('Must provide unique id')

            if not name:
                name = unique_id

            response = self.custom_create_pipeline(
                name, unique_id, description, tags)
            self.pipeline_id = response['pipelineId']

    @property
    def id(self):
        """Fetch the id of the datapipeline

        Returns:
            id(str): id of the datapipeline
        """
        return self.pipeline_id

    @property
    def aws_format(self):
        """Create a list aws readable format dicts of all pipeline objects

        Returns:
            result(list of dict): list of AWS-readable dict of all objects
        """
        return [x.aws_format() for x in self.objects]

    def add_object(self, pipeline_object):
        """Add an object to the datapipeline

        Args:
            pipeline_object(): datapipeline object to be added
        """
        if not isinstance(pipeline_object, PipelineObject):
            raise ETLInputError(
                'pipeline object must be of the type PipelineObject')

        self.objects.append(pipeline_object)

    def validate_pipeline_definition(self):
        """Validate the current pipeline
        """
        response = self.conn.validate_pipeline_definition(
            self.aws_format, self.id)
        return response.get('validationErrors', None)

    def update_pipeline_definition(self):
        """Updates the datapipeline definition
        """
        self.conn.put_pipeline_definition(self.aws_format, self.id)

    def activate(self):
        """Activate the datapipeline
        """
        self.conn.activate_pipeline(self.id)

    def delete(self):
        """Deletes the datapipeline
        """
        self.conn.delete_pipeline(self.pipeline_id)

    def instance_details(self):
        """List details of all the pipeline instances

        Returns:
            result(dict of list): Dictionary mapping run date to a list of
            pipeline instances combined per date
        """
        # Get instances associated with the pipeline id
        instances = list_pipeline_instances(self.pipeline_id, self.conn)

        # Collect instances by start date
        result = defaultdict(list)
        for instance in instances:
            result[instance['@scheduledStartTime']].append(instance)
        return result

    def custom_create_pipeline(self, name, unique_id, description=None,
                               tags=None):
        """
        Creates a new empty pipeline. Adds tags feature not yet available in
        boto

        Args:
            tags(list(dict)): a list of tags in the format
                              [{key: foo, value: bar}]
        """
        params = {'name': name, 'uniqueId': unique_id, }
        if description is not None:
            params['description'] = description
        if tags is not None:
            params['tags'] = tags
        return self.conn.make_request(action='CreatePipeline',
                                      body=json.dumps(params))
