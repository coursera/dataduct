"""Tests for helper functions
"""
import os

from unittest import TestCase
from ..helpers import stringify_credentials
from ..helpers import parse_path
from nose.tools import eq_


class TestHelpers(TestCase):
    """Tests for helper functions
    """
    @staticmethod
    def test_stringify_credentials_simple():
        """Tests that stringify_credentials can serialize creds
        """
        result = stringify_credentials('a', 'b')
        eq_(result, 'aws_access_key_id=a;aws_secret_access_key=b')

    @staticmethod
    def test_stringify_credentials_with_token():
        """Tests that stringify_credentials can serialize creds
        """
        result = stringify_credentials('a', 'b', 'c')
        eq_(result, 'aws_access_key_id=a;aws_secret_access_key=b;token=c')

    @staticmethod
    def test_parse_path_none_returns_none():
        """Tests that giving parse_path a None path returns None
        """
        eq_(parse_path(None), None)
        eq_(parse_path(None, 'test'), None)

    @staticmethod
    def test_parse_path_abs_path_returns_itself():
        """Tests that giving parse_path an absolute path returns itself
        """
        eq_(parse_path('/abs/path'), '/abs/path')

    @staticmethod
    def test_parse_path_relative_path_returns_new_path():
        """Tests that a relative path gets transformed
        """
        from dataduct.config import Config
        config = Config()
        config.etl['TEST_PATH'] = '/transform'
        eq_(parse_path('test/path', 'TEST_PATH'), '/transform/test/path')

    @staticmethod
    def test_parse_path_relative_path_no_matching_config_returns_itself():
        """Tests that the original path is returned if no matching
        transformation can be found
        """
        from dataduct.config import Config
        config = Config()
        config.etl.pop('TEST_PATH', None)
        eq_(parse_path('test/path', 'TEST_PATH'), 'test/path')

    @staticmethod
    def test_parse_path_expands_user():
        """Tests that parse_path expands the user symbol
        """
        from dataduct.config import Config
        config = Config()
        config.etl['TEST_PATH'] = '~/transform'
        eq_(
            parse_path('test/path', 'TEST_PATH'),
            os.path.expanduser('~/transform/test/path'),
        )
