"""
Class for storing a S3 Log Path
"""

from os.path import join
from .s3_path import S3Path


class S3LogPath(S3Path):
    """S3 Log path for data pipeline
    S3LogPath only exists to correct the use of S3 URI's by Data
    Pipeline. In most cases, one should use a backslash to disambiguate
    prefixes. For instance, the former prefix includes the latter
    unless there is a backslash:

    ::
        s3:://coursera-bucket/dev
        s3:://coursera-bucket/dev_log_dir

    However, if one adds a backslash to the log s3 URI, Data Pipeline
    will add another backslash before adding subdirectories. These
    double backslashes break boto.
    """
    def __init(self, **kwargs):
        """Constructor for S3LogPath
        """
        super(S3LogPath, self).__init__(**kwargs)

    @property
    def uri(self):
        """Get the log directory path

        Returns:
            s3_uri(str): s3_log path without the trailing '/'
        """
        if self.key is None:
            return None
        return join('s3://', self.bucket, self.key).rstrip('/')
