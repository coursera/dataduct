"""
Base class for data pipeline objects
"""
from collections import defaultdict

from ..s3.s3_path import S3Path
from ..s3.s3_file import S3File
from ..s3.s3_directory import S3Directory


class PipelineObject(object):
    """DataPipeline class with steps and metadata.

    The pipeline object has a one-to-one mapping with the AWS pipeline objects,
    which are described at the following url:
        http://docs.aws.amazon.com/datapipeline/latest/
            DeveloperGuide/dp-pipeline-objects.html
    The pipeline object acts much like a dictionary (with similar getters and
    setters) which provides access to all aws attributes.

    """
    def __init__(self, id, **kwargs):
        """Constructor for the pipeline object

        Note:
            We ignore the kwargs as those are required to be specified by
            the base classes

        Args:
            id (str): id of the pipeline object
             **kwargs: Arbitrary keyword arguments.

        """
        self.id = id
        self.fields = defaultdict(list)
        # additional s3 files that may not appear as an AWS field
        self.additional_s3_files = []

    @property
    def s3_files(self):
        """Fetch the list of files associated with the pipeline object
        Returns:
            result(list of S3Files): List of files to be uploaded to s3
        """
        result = self.additional_s3_files[:]
        for _, values in self.fields.iteritems():
            for value in values:
                if isinstance(value, S3File) or isinstance(value, S3Directory):
                    result.append(value)
        return result

    def __getitem__(self, key):
        """Fetch the items associated with a key
        Note: This value will be a list if there are multiple values
            associated with one key. Otherwise, it will be the singleton item.
        Args:
            key(str): Key of the item to be fetched
        Returns:
            result(list or singleton): value(s) associated with the key
        """
        if key in ['id', 'name']:
            return self.id
        result = self.fields.get(key, None)
        if result is None:
            return None
        if len(result) == 1:
            return result[0]
        return result

    def __delitem__(self, key):
        """Delete the key from object fields
        Args:
            key(str): Key of the item to be fetched
        """
        self.fields.pop("key", None)

    def __setitem__(self, key, value):
        """Set an key value field
        Args:
            key(str): Key of the item to be fetched
            value: Value of the item to be fetched
        """
        # Do not add none values
        if value is None:
            return

        self.fields[key].extend(list(value))
        if key == 'dependsOn':
            self.fields[key] = list(set(self.fields[key]))

    def aws_format(self):
        """
        Output: the AWS-readable dict format.
        """
        fields = []
        for key, values in self.fields.iteritems():
            for value in values:
                if isinstance(value, PipelineObject):
                    fields.append({'key': key, 'refValue': value.id})
                elif isinstance(value, S3Path):
                    fields.append({'key': key, 'stringValue': value.uri})
                elif isinstance(value, S3File) or \
                     isinstance(value, S3Directory):
                    fields.append({'key': key,
                                   'stringValue': value.s3_path.uri})
                else:
                    fields.append({'key': key, 'stringValue': str(value)})
        return {'id': self.id, 'name': self.id, 'fields': fields}
