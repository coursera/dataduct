"""
Base class for storing a S3 File
"""
from .s3_path import S3Path
from .utils import upload_dir_to_s3


class S3Directory(object):
    """S3 Directory object helps operate with a directory on S3

    The S3Directory acts much like the S3File.
    It represents a directory. Tries to unify the concept of a directory
    stored locally with one stored in S3.

    """
    def __init__(self, path=None, s3_path=None):
        """Constructor for the S3 File object

        Args:
            path (str): Local path to file
            s3_path (S3Path, optional): s3_path of the file

        """
        self.path = path
        self._s3_path = s3_path

    @property
    def s3_path(self):
        """Outputs the s3_path
        """
        return self._s3_path

    @s3_path.setter
    def s3_path(self, value):
        """Set the S3 path for the file

        Args:
            value(S3Path): s3path of the directory
        """
        assert isinstance(value, S3Path), 'input path must be of type S3Path'
        assert value.is_directory, 'input path must be a directory'
        self._s3_path = value

    def upload_to_s3(self):
        """Uploads the directory to the s3 directory
        """
        upload_dir_to_s3(self._s3_path, self.path)
