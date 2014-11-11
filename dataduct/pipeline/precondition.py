"""
Pipeline object class for the precondition step
"""

from .pipeline_object import PipelineObject


class Precondition(PipelineObject):
    """Precondition object added to all pipelines
    """

    def __init__(self,
                 id,
                 is_directory=True,
                 **kwargs):
        """Constructor for the Precondition class

        Args:
            id(str): id of the precondition object
            is_directory(bool): if s3 path is a directory or not
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if is_directory:
            super(Precondition, self).__init__(
                id=id,
                type='S3PrefixNotEmpty',
                s3Prefix="#{node.directoryPath}",
            )
        else:
            super(Precondition, self).__init__(
                id=id,
                type='S3KeyExists',
                s3Prefix="#{node.filePath}",
            )
