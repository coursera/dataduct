"""
Script that has action functions for config
"""
from .config import Config
from ..s3 import S3Path
from ..s3 import S3File

from .constants import CONFIG_STR
from .constants import DATADUCT_CFG_FILE


config = Config()

def s3_config_path():
    """S3 uri for the config files
    """
    key = [config.etl.get('S3_BASE_PATH', ''), CONFIG_STR, DATADUCT_CFG_FILE]
    return S3Path(bucket=config.etl['S3_ETL_BUCKET'], key=key)


def sync_to_s3():
    """Upload the config file to an S3 location
    """
    s3_file = S3File(text=config.raw_config(), s3_path=s3_config_path())
    s3_file.upload_to_s3()


def sync_from_s3(filename):
    """Read the config file from S3
    """
    s3_file = S3File(s3_path=s3_config_path())
    text = s3_file.text

    if filename is None:
        print text
    else:
        with open(filename, 'w') as op_file:
            op_file.write(text)
