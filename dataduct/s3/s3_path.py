"""
Base class for storing a S3 Path
"""
from os.path import join
from re import findall
from re import sub

from ..utils.exceptions import ETLInputError


class S3Path(object):
    """S3 Path object that provides basic functions to interact with an S3 path

    The s3 path ensures that there is a regular way of representing paths in
    s3, and distinguishing between directories and files.

    Note:
        We don't connect with S3 using boto for any checks here.

    """
    def __init__(self, key=None, bucket=None, uri=None, parent_dir=None,
                 is_directory=False):
        """Constructor for the S3 Path object

        If key is specified then bucket needs to be specified as well.
        You can only specify either uri or key & bucket pair

        Either form is acceptable, but the two should not be mixed. Choose one
        convention to document the __init__ method and be consistent with it.

        Args:
            key (str): Key for the S3 path
            bucket (str): Bucket of the key provided
            uri (str): Complete uri of the S3 path
            is_directory (bool): Is the specified S3 path a directory

        """

        if parent_dir and not isinstance(parent_dir, S3Path):
            raise ETLInputError('parent_dir must be S3Path')
        if parent_dir and not parent_dir.is_directory:
            raise ETLInputError('parent_dir must be a directory')

        # Initialize all the values
        self.is_directory = is_directory
        self.key = None

        if uri is not None:
            if key or parent_dir:
                raise ETLInputError('Key or parent_dir given with uri')
            self.bucket = findall(r's3://([^/]+)', uri)[0]
            self.key = sub(r's3://[^/]+/', '', uri.rstrip('/'))
        elif parent_dir is not None:
            self.bucket = parent_dir.bucket
            self.key = parent_dir.key
            self.is_directory = True
            if key:
                self.append(key, is_directory)
        else:
            self.append(key, is_directory)
            self.bucket = bucket

    def append(self, new_key, is_directory=False):
        """Appends new key to the current key

        Args:
            new_key (str): Key for the S3 path
            is_directory (bool): Is the specified S3 path a directory

        """
        assert self.is_directory or self.key is None, \
            'Can only append to path that is directory'

        # If new key is list we want to flatten it out
        if isinstance(new_key, list):
            new_key = join(*new_key)

        # Remove duplicate, leading, and trailing '/'
        new_key = [a for a in new_key.split("/") if a != '']

        # AWS prevents us from using periods in paths
        # Substitute them with '_'
        if is_directory:
            directory_path = new_key
            file_name = ''
        else:
            directory_path = new_key[:-1]
            file_name = new_key[-1]

        # Remove periods
        new_key = [sub(r'\.', '_', a) for a in directory_path]
        new_key.append(file_name)
        new_key = join(*new_key)

        if self.key:
            self.key = join(self.key, new_key)
        else:
            self.key = new_key
        self.is_directory = is_directory

    @property
    def uri(self):
        """
        Returns the uri of the S3 path

        Note:
            Note that if there is a directory, the URI is appended a '/'

        Returns:
            S3 URI
        """
        path = join('s3://', self.bucket, self.key)
        if self.is_directory and not path.endswith('/'):
            path += '/'
        return  path

    @property
    def base_filename(self):
        """
        Returns the base_filename of the S3 path
        Returns:
            filename(String): Base filename of the s3 path
        """
        if self.is_directory:
            raise ETLInputError('No base filename for directories')
        return self.key.split('/')[-1]
