"""
Shared utility functions
"""
import boto.s3
import os

from .s3_path import S3Path


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
        key_name = os.path.join(s3_path.key, os.path.basename(file_name))
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
        raise_when_no_exist(bool, optional): Raise error if file not found

    Raises:
        ValueError: If s3_old_path does not exist
    """
    bucket = get_s3_bucket(s3_old_path.bucket)
    key = bucket.get_key(s3_old_path.key)
    if key:
        key.copy(s3_new_path.bucket, s3_new_path.key)

    if raise_when_no_exist and not key:
        raise ValueError('The key does not exist: %s' % s3_old_path.uri)


def upload_dir_to_s3(s3_path, local_path, filter_function=None):
    """Uploads a complete directory to s3

    Args:
        s3_path(S3Path): Output path of the file to be uploaded
        local_path(file_path): Input path of the file to be uploaded
        filter_function(function): Function to filter out directories
    """
    assert isinstance(s3_path, S3Path), 'input path should be of type S3Path'
    assert s3_path.is_directory, 'S3 path must be directory'
    assert os.path.isdir(local_path), 'Local path must be a directory'

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
    assert isinstance(s3_path, S3Path), 'input path should be of type S3Path'
    assert s3_path.is_directory, 'S3 path must be directory'

    bucket = get_s3_bucket(s3_path.bucket)
    keys = bucket.get_all_keys(prefix=s3_path.key + '/')

    # Download each file individually
    for key in keys:
        # Calculate relative path
        local_file_path = os.path.join(
            local_path, str(key.name[len(s3_path.key) + 1:]))

        # Make sure directories exist
        local_file_dir = os.path.dirname(local_file_path)
        if not os.path.exists(local_file_dir):
            os.makedirs(local_file_dir)

        with open(local_file_path, 'w') as f:
            key.get_contents_to_file(f)


def delete_dir_from_s3(s3_path):
    """Deletes a complete directory from s3

    Args:
        s3_path(S3Path): Path of the directory to be deleted
    """
    assert isinstance(s3_path, S3Path), 'input path should be of type S3Path'
    assert s3_path.is_directory, 'S3 path must be directory'

    bucket = get_s3_bucket(s3_path.bucket)
    prefix = s3_path.key

    # Enforce this to be a folder's prefix
    if not prefix.endswith('/'):
        prefix += '/'
    keys = bucket.get_all_keys(prefix=s3_path.key)
    for key in keys:
        key.delete()
