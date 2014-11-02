"""
Base class for data pipeline objects
"""

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
        self.fields = {}
        # additional s3 files that may not appear as an AWS field
        self.additional_s3_files = []

    @property
    def s3_files(self):
        """
        Output: returns all fields which represent s3 files.
        """
        result = self.additional_s3_files[:]
        for _, values in self.fields.iteritems():
            for value in values:
                if isinstance(value, S3File) or isinstance(value, S3Dir):
                    result.append(value)
        return result
