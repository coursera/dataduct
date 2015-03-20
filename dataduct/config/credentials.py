"""Credentials utility functions for connecting to various services
"""
import os
import requests
import sys
from ConfigParser import SafeConfigParser


def get_aws_credentials_from_iam():
    """Get aws credentials using the IAM api
    Note: this script only runs on an EC2 instance with the appropriate
        resource roles. For more information, see the following:
        http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/\
        AESDG-chapter-instancedata.html

    Returns:
        access_key(str): AWS access key
        secret_key(str): AWS secret key
        token(str): Connection token
    """
    url = 'http://169.254.169.254/latest/meta-data/iam/security-credentials/'

    # Get role name
    r = requests.get(url)

    if not r.ok:
        raise Exception('Request failed for url %s.' % url)

    # Add role name to url
    url += r.content

    # Get access keys
    r = requests.get(url)
    if not r.ok:
        raise Exception('Request failed for url %s.' % url)

    json_result = r.json()
    return (json_result['AccessKeyId'],
            json_result['SecretAccessKey'],
            json_result['Token'])


def get_aws_credentials_from_file(filename=None):
    """Get the aws from credential files
    """
    config = SafeConfigParser()
    cred_file = None
    if filename is not None and os.path.isfile(filename):
        cred_file = filename
    elif os.path.isfile('/etc/boto.cfg'):
        cred_file = '/etc/boto.cfg'
    elif os.path.isfile(os.path.expanduser('~/.boto')):
        cred_file = os.path.expanduser('~/.boto')
    elif os.path.isfile(os.path.expanduser('~/.aws/credentials')):
        cred_file = os.path.expanduser('~/.aws/credentials')
    else:
        raise Exception("Cannot find a credentials file")

    config.read(cred_file)
    aws_access_key_id = config.get('Credentials',
                                   'aws_access_key_id')
    aws_secret_access_key = config.get('Credentials',
                                       'aws_secret_access_key')
    return (aws_access_key_id, aws_secret_access_key, None)


def get_aws_credentials(filename=None):
    """Get the aws credentials from IAM or files
    """
    try:
        aws_key, aws_secret, token = get_aws_credentials_from_iam()
    except Exception, error:
        sys.stderr.write('Failed to get creds from IAM: %s \n' % error.message)
        aws_key, aws_secret, token = get_aws_credentials_from_file(filename)

    return aws_key, aws_secret, token
