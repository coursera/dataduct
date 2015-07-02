"""
Base class for data pipeline objects
"""
from collections import defaultdict

from ..s3 import S3Directory
from ..s3 import S3File
from ..s3 import S3Path
from ..utils.exceptions import ETLInputError


class PipelineObject(object):
    """DataPipeline class with steps and metadata.

    The pipeline object has a one-to-one mapping with the AWS pipeline objects,
    which are described at the following url:
    http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-pipeline-objects.html
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
        self._id = id
        self.fields = defaultdict(list)

        for key, value in kwargs.iteritems():
            if value is not None:
                self[key] = value

        # additional s3 files that may not appear as an AWS field
        self.additional_s3_files = []

    @property
    def id(self):
        """Fetch the id of the pipeline object

        Returns:
            id(str): id of the pipeline object
        """
        return self._id

    @property
    def s3_files(self):
        """Fetch the list of files associated with the pipeline object

        Returns:
            result(list of S3Files): List of files to be uploaded to s3
        """
        result = self.additional_s3_files
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
            return self._id

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
        self.fields.pop(key, None)

    def __setitem__(self, key, value):
        """Set an key value field

        Args:
            key(str): Key of the item to be fetched
            value: Value of the item to be fetched
        """
        # Store value as a list if there is only one
        if not isinstance(value, list):
            value = [value]

        # Do not add none values
        self.fields[key].extend([x for x in value if x is not None])
        if key == 'dependsOn':
            self.fields[key] = list(set(self.fields[key]))

    def add_additional_files(self, new_files):
        """Add new s3 files

        Args:
            new_files(S3File): list of new S3 files for the activity
        """
        if new_files is None:
            return

        for new_file in new_files:
            if not isinstance(new_file, S3File):
                raise ETLInputError('File must be an S3 File object')
            self.additional_s3_files.append(new_file)

    def aws_format(self):
        """Create the aws readable format of object

        Returns:
            result: The AWS-readable dict format of the object
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
        return {'id': self._id, 'name': self._id, 'fields': fields}
