"""
Shared utility functions
"""
import boto.s3
from os.path import join
from os.path import basename

from s3_path import S3Path


def get_s3_bucket(bucket_name):
    """Returns an S3 bucket object from boto

    Args:
        bucket_name(str): Name of the bucket to be read
    Returns:
        bucket(boto.S3.bucket.Bucket): Boto S3 bucket object
    """

    bucket = boto.s3.bucket.Bucket(boto.connect_s3(), bucket_name)
    return bucket


def read_from_s3(s3_path):
    """Reads the contents of a file from S3

    Args:
        s3_path(S3Path): Input path of the file to be read
    Returns:
        results(str): Contents of the file as a string
    """
    assert isinstance(s3_path, S3Path), 'input path should be of type S3Path'

    bucket = get_s3_bucket(s3_path.backet)
    key = bucket.get_key(s3_path.key)

    return key.get_contents_as_string()


def upload_to_s3(s3_path, file_name=None, file_text=None):
    """Uploads a file to S3

    Args:
        s3_path(S3Path): Output path of the file to be uploaded
        file_name(str): Name of the file to be uploaded to s3
        file_text(str): Contents of the file to be uploaded
    """
    assert isinstance(s3_path, S3Path), 'input path should be of type S3Path'
    assert any([file_name, file_text]), 'file_name or text should be given'

    bucket = get_s3_bucket(s3_path.backet)
    if s3_path.is_directory:
        key_name = join(s3_path.key, basename(file_name))
    else:
        key_name = s3_path.key

    key = bucket.new_key(key_name)
    if file_name:
        key.set_contents_from_filename(file_name)
    else:
        key.set_contents_from_string(file_text)


def copy_within_s3(s3_old_path, s3_new_path, raise_when_no_exist=True):
    """Copies files from one S3 Path to another

    Args:
        s3_old_path(S3Path): Output path of the file to be uploaded
        s3_new_path(S3Path): Output path of the file to be uploaded
        raise_when_no_exist(bool): Raise

    Raises:

    """
    bucket = get_s3_bucket(s3_old_path.bucket)
    key = bucket.get_key(s3_old_path.key)
    if key:
        key.copy(s3_new_path.bucket, s3_new_path.key)

    if raise_when_no_exist and not key:
        raise ValueError('The key does not exist: %s' % s3_old_path.uri)
