"""
Base class for storing a S3 Path
"""
from os.path import join
from re import findall
from re import sub


def validate_inputs(key, bucket, uri):
    """Checks the input params are correct by parsing key, bucket and uri

    Args:
        key (str): Key for the S3 path
        bucket (str): Bucket of the key provided
        uri (str): Complete uri of the S3 path

    Raises:
        ValueError: if the inputs aren't as expected

    """
    if key is not None and bucket is None:
        raise ValueError('bucket should be specified if key is not none')
    if key is None and bucket is not None:
        raise ValueError('key should be specified if bucket is not none')
    if key is not None and bucket is not None and uri is not None:
        raise ValueError('key, bucket and uri can not all be provided')
    if key is None and bucket is None and uri is None:
        raise ValueError('uri or key & bucket should be provided')


class S3Path(object):
    """S3 Path object that provides basic functions to interact with an S3 path

    The s3 path ensures that there is a regular way of representing paths in
    s3, and distinguishing between directories and files.

    Note:
        We don't connect with S3 using boto for any checks here.

    """
    def __init__(self, key=None, bucket=None, uri=None, is_directory=False):
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

        validate_inputs(key, bucket, uri)

        # Initialize all the values
        self.is_directory = is_directory

        if uri is not None:
            self.bucket = findall(r's3://([^/]+)', uri)[0]
            self.key = sub(r's3://[^/]+/', '', uri.rstrip('/'))
        else:
            self.key = key
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
        new_key = [a for a in new_key.split("/") if a != ""]

        # TODO: AWS BUG prevents us from using periods in paths.
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
        return join('s3://', self.bucket, self.key) + \
            ('/' if self.is_directory else '')
