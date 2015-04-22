"""Tests that the config singleton is working properly
"""
from os.path import expanduser
from os.path import join

from unittest import TestCase
from mock import patch
from testfixtures import TempDirectory
from nose.tools import eq_
from nose.tools import raises

from ..config import get_config_files
from ..config import load_yaml


class TestConfig(TestCase):
    """Tests for config singleton
    """
    def setUp(self):
        self.test_yaml_file = '\n'.join([
            'test:',
            '    test_sub:',
            '    -   test_sub1: foo',
            '        test_sub1_other: bar',
            '    -   test_sub2: foobar',
        ])
        self.test_config_dict = {
            'test': {
                'test_sub': [
                    {
                        'test_sub1': 'foo',
                        'test_sub1_other': 'bar',
                    },
                    {
                        'test_sub2': 'foobar',
                    }
                ]
            }
        }

    @staticmethod
    @patch.dict('os.environ', {}, clear=True)
    def test_get_config_files_no_enviroment_variable():
        """Tests that correct config file paths are returned when there's no
        enviroment variable
        """
        expected = [
            '/etc/dataduct.cfg',
            expanduser('~/.dataduct/dataduct.cfg'),
        ]
        result = get_config_files()
        eq_(result, expected)

    @staticmethod
    @patch.dict('os.environ', {'DATADUCT_CONFIG_PATH': '/test/test.cfg'})
    def test_get_config_files_with_enviroment_variable():
        """Tests that correct config file paths are returned when there is
        an enviroment variable
        """
        expected = [
            '/etc/dataduct.cfg',
            expanduser('~/.dataduct/dataduct.cfg'),
            '/test/test.cfg',
        ]
        result = get_config_files()
        eq_(result, expected)

    def test_load_yaml_works_correctly(self):
        """Tests that the yaml file can be loaded correctly
        """
        with TempDirectory() as d:
            d.write('test.yaml', self.test_yaml_file)
            result = load_yaml([join(d.path, 'test.yaml')])
            eq_(result, self.test_config_dict)

    @staticmethod
    @raises(IOError)
    def test_no_config_file_raises():
        """Tests that an exception is raised if no yaml file path is passed in
        """
        load_yaml([])

    @staticmethod
    @raises(IOError)
    def test_cannot_find_config_file_raises():
        """Tests that an exception is raised if it cannot find any yaml files
        """
        with TempDirectory() as d:
            with TempDirectory() as d2:
                load_yaml([join(d.path, 'test.cfg'),
                           join(d2.path, 'test.cfg')])
