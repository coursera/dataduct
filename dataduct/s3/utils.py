"""
Shared utility functions
"""
import boto.s3
import os

from ..utils.exceptions import ETLInputError
from .s3_path import S3Path


def get_s3_bucket(bucket_name):
    """Returns an S3 bucket object from boto

    Args:
        bucket_name(str): Name of the bucket to be read

    Returns:
        bucket(boto.S3.bucket.Bucket): Boto S3 bucket object
    """
    conn = boto.connect_s3()
    bucket = boto.s3.bucket.Bucket(conn, bucket_name)
    return bucket


def read_from_s3(s3_path):
    """Reads the contents of a file from S3

    Args:
        s3_path(S3Path): Input path of the file to be read

    Returns:
        results(str): Contents of the file as a string
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    bucket = get_s3_bucket(s3_path.bucket)
    key = bucket.get_key(s3_path.key)

    return key.get_contents_as_string()


def upload_to_s3(s3_path, file_name=None, file_text=None):
    """Uploads a file to S3

    Args:
        s3_path(S3Path): Output path of the file to be uploaded
        file_name(str): Name of the file to be uploaded to s3
        file_text(str): Contents of the file to be uploaded
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    if not any([file_name, file_text]):
        raise ETLInputError('File_name or text should be given')

    bucket = get_s3_bucket(s3_path.bucket)
    if s3_path.is_directory:
        key_name = os.path.join(s3_path.key, os.path.basename(file_name))
    else:
        key_name = s3_path.key

    key = bucket.new_key(key_name)
    if file_name:
        key.set_contents_from_filename(file_name)
    else:
        key.set_contents_from_string(file_text)


def download_from_s3(s3_path, local_path):
    """Downloads a file from s3

    Args:
        s3_path(S3Path): Input path of the file to be downloaded
        local_path(file_path): Output path of the file to be downloaded
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    if s3_path.is_directory:
        raise ETLInputError('S3 path must not be directory')

    bucket = get_s3_bucket(s3_path.bucket)
    key = bucket.get_key(key_name=s3_path.key)

    # Make sure directories exist
    if not os.path.isdir(os.path.abspath(local_path)):
        os.makedirs(local_path)

    # Calculate relative path
    local_file_path = os.path.join(local_path, s3_path.base_filename)
    key.get_contents_to_filename(local_file_path)


def copy_within_s3(s3_old_path, s3_new_path, raise_when_no_exist=True):
    """Copies files from one S3 Path to another

    Args:
        s3_old_path(S3Path): Output path of the file to be uploaded
        s3_new_path(S3Path): Output path of the file to be uploaded
        raise_when_no_exist(bool, optional): Raise error if file not found

    Raises:
        ETLInputError: If s3_old_path does not exist
    """
    bucket = get_s3_bucket(s3_old_path.bucket)
    key = bucket.get_key(s3_old_path.key)
    if key:
        key.copy(s3_new_path.bucket, s3_new_path.key)
    elif raise_when_no_exist:
        raise ETLInputError('The key does not exist: %s' % s3_old_path.uri)


def upload_dir_to_s3(s3_path, local_path, filter_function=None):
    """Uploads a complete directory to s3

    Args:
        s3_path(S3Path): Output path of the file to be uploaded
        local_path(file_path): Input path of the file to be uploaded
        filter_function(function): Function to filter out directories
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    if not s3_path.is_directory:
        raise ETLInputError('S3 path must be directory')

    if not os.path.isdir(local_path):
        raise ETLInputError('Local path must be a directory')

    bucket = get_s3_bucket(s3_path.bucket)

    # Add each file individually
    for root, _, file_names in os.walk(local_path, followlinks=True):
        for file_name in file_names:
            # Filter file_name based on filter function
            if filter_function and not filter_function(file_name):
                continue

            local_file_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(local_file_path, local_path)

            key_string = os.path.join(s3_path.key, relative_path)
            key = bucket.new_key(key_string)
            key.set_contents_from_filename(local_file_path)


def download_dir_from_s3(s3_path, local_path):
    """Downloads a complete directory from s3

    Args:
        s3_path(S3Path): Input path of the file to be downloaded
        local_path(file_path): Output path of the file to be downloaded
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    if not s3_path.is_directory:
        raise ETLInputError('S3 path must be directory')

    bucket = get_s3_bucket(s3_path.bucket)
    keys = bucket.get_all_keys(prefix=s3_path.key)

    # Download each file individually
    for key in keys:
        # Calculate relative path
        local_file_path = os.path.join(
            local_path, os.path.basename(str(key.name)))

        # Make sure directories exist
        local_file_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)

        key.get_contents_to_filename(local_file_path)


def delete_dir_from_s3(s3_path):
    """Deletes a complete directory from s3

    Args:
        s3_path(S3Path): Path of the directory to be deleted
    """
    if not isinstance(s3_path, S3Path):
        raise ETLInputError('Input path should be of type S3Path')

    if not s3_path.is_directory:
        raise ETLInputError('S3 path must be directory')

    bucket = get_s3_bucket(s3_path.bucket)
    prefix = s3_path.key

    # Enforce this to be a folder's prefix
    prefix += '/' if not prefix.endswith('/') else ''

    keys = bucket.get_all_keys(prefix=s3_path.key)
    for key in keys:
        key.delete()


def copy_dir_with_s3(s3_old_path, s3_new_path, raise_when_no_exist=True):
    """Copies files from one S3 Path to another

    Args:
        s3_old_path(S3Path): Output path of the file to be uploaded
        s3_new_path(S3Path): Output path of the file to be uploaded
        raise_when_no_exist(bool, optional): Raise error if file not found

    Raises:
        ETLInputError: If s3_old_path does not exist
    """
    if not isinstance(s3_old_path, S3Path):
        raise ETLInputError('S3 old path should be of type S3Path')

    if not s3_old_path.is_directory:
        raise ETLInputError('S3 old path must be directory')

    if not isinstance(s3_new_path, S3Path):
        raise ETLInputError('S3 new path should be of type S3Path')

    if not s3_new_path.is_directory:
        raise ETLInputError('S3 new path must be directory')

    bucket = get_s3_bucket(s3_old_path.bucket)
    prefix = s3_old_path.key

    # Enforce this to be a folder's prefix
    prefix += '/' if not prefix.endswith('/') else ''

    keys = bucket.get_all_keys(prefix=s3_old_path.key)
    for key in keys:
        if key:
            key.copy(s3_new_path.bucket,
                     os.path.join(s3_new_path.key, os.path.basename(key.key)))
        elif raise_when_no_exist:
            raise ETLInputError('The key does not exist: %s' % s3_old_path.uri)
