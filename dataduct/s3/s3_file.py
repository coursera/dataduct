"""
Base class for storing a S3 File
"""
from ..utils.exceptions import ETLInputError
from ..utils.helpers import parse_path
from .s3_path import S3Path
from .utils import read_from_s3
from .utils import upload_to_s3

DEFAULT_FILE_NAME = 'file'


class S3File(object):
    """S3 File object that provides functions to operate with a file on S3

   The S3 file unifies the file concept, which could be stored on the
   local file system, as a string, or already in s3.

    """
    def __init__(self, path=None, text=None, s3_path=None):
        """Constructor for the S3 File object

        Args:
            path (str): Local path to file
            text (str): Text of a file
            s3_path (S3Path, optional): s3_path of the file

        """

        if path or text:
            assert (path and not text) or (text and not path), \
                'Cannot specify both path and text for s3 file.'

        # Initialize all the values
        self._path = parse_path(path)
        self._text = text
        self._s3_path = s3_path

    def upload_to_s3(self):
        """Sends file to URI. This action is idempotent.

        Raises:
            ETLInputError: If no URL is provided
        """
        if self._s3_path:
            if self._path or self._text:
                # There exists something locally to store
                upload_to_s3(self._s3_path, self._path, self._text)
        else:
            raise ETLInputError('No URI provided for the file to be uploaded')

    @property
    def text(self):
        """Outputs the text of the associated file

        Returns:
            result(str): The text of the file. Can be local or on S3
        """
        if self._text:
            # The text attribute is populated; return it.
            return self._text
        elif self._path:
            # The path attribute is populated. Read the file contents
            with open(self._path, 'r') as f:
                return f.read()
        return read_from_s3(self._s3_path)

    @property
    def file_name(self):
        """The file name of this file

        Returns:
            file_name(str): The file_name of this file
        """
        if self._path:
            return self._path.split('/').pop()
        else:
            return DEFAULT_FILE_NAME

    @property
    def s3_path(self):
        """Outputs the s3_path
        """
        return self._s3_path

    @s3_path.setter
    def s3_path(self, s3_path):
        """Set the S3 path for the file

        Args:
            s3_path(S3Path): If the path is a directory, a
            name will be assigned based on the path variable.
            If there is no path, the name "file" will be applied.
        """

        if not isinstance(s3_path, S3Path):
            raise ETLInputError('Input path should be of type S3Path')

        # Copy the object as we would change it for the file
        self._s3_path = S3Path(
            key=s3_path.key,
            bucket=s3_path.bucket,
            is_directory=s3_path.is_directory,
        )
        if s3_path.is_directory:
            # This is a directory; add a file name.
            self._s3_path.append(self.file_name)
