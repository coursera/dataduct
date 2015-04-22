"""Tests for the connection file
"""
from unittest import TestCase
from nose.tools import eq_
from nose.tools import raises

from ...config import Config
from ...utils.exceptions import ETLConfigError
from .. import connection


class TestConnection(TestCase):
    """Tests for the connection file
    """
    @staticmethod
    def test_get_redshift_config_correctly_returns():
        """Tests that get_redshift_config can successfully retrieve the
        redshift config
        """
        config = Config()
        config.redshift = 'test'
        eq_(connection.get_redshift_config(), 'test')

    @staticmethod
    @raises(ETLConfigError)
    def test_get_redshift_config_no_config_raises():
        """Tests that get_redshift_config raises an exception if the redshift
        config cannot be found
        """
        config = Config()
        del config.redshift
        connection.get_redshift_config()

    @staticmethod
    @raises(ETLConfigError)
    def test_sql_config_no_config_raises():
        """Tests that get_sql_config raises an exception if the config cannot
        be found
        """
        config = Config()
        del config.mysql
        connection.get_sql_config('test')

    @staticmethod
    @raises(ETLConfigError)
    def test_sql_config_cannot_find_hostname_raises():
        """Tests that get_sql_config raises an exception if the hostname is not
        in the config
        """
        config = Config()
        config.mysql = {'test': {}}
        connection.get_sql_config('test1')

    @staticmethod
    def test_sql_config_correctly_returns():
        """Tests that get_sql_config can correctly retrieve the config
        """
        config = Config()
        config.mysql = {'test': {'cred': 'data'}}
        result = connection.get_sql_config('test')
        eq_(result['DATABASE'], 'test')
        eq_(result['cred'], 'data')
