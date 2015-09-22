"""Tests that the config actions are working properly
"""
from unittest import TestCase
from nose.tools import eq_

from .. import config_actions
from ..config import Config


class TestConfigActions(TestCase):
    """Tests for config actions
    """
    @staticmethod
    def test_s3_config_path():
        """Tests that s3_config_path correctly returns the S3 base path
        """
        config = Config()
        config.etl['S3_BASE_PATH'] = 'test/path'
        config.etl['S3_ETL_BUCKET'] = 'test_bucket'
        config_actions.CONFIG_STR = 'test_config_str'
        config_actions.CFG_FILE = 'test_cfg_file.cfg'
        result = config_actions.s3_config_path()
        eq_(result.bucket, 'test_bucket')
        eq_(result.key, 'test/path/test_config_str/test_cfg_file.cfg')
