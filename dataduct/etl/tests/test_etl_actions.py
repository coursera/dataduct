"""Tests for the ETL actions
"""
import os

import unittest
from testfixtures import TempDirectory
from nose.tools import raises
from nose.tools import eq_

from ..etl_actions import read_pipeline_definition
from ..etl_actions import create_pipeline
from ...utils.exceptions import ETLInputError


class EtlActionsTests(unittest.TestCase):
    """Tests for the ETL actions
    """

    def setUp(self):
        """Setup text fixtures
        """
        self.load_hour = '01'
        self.load_min = '23'
        load_time = self.load_hour + ':' + self.load_min
        self.test_yaml = '\n'.join([
            'name: example_load_redshift',
            'frequency: one-time',
            'load_time: ' + load_time,
            'max_retries: 5',
            'description: Example for the load_redshift step',
            'steps:',
            '-   step_type: extract-local',
            '    path: data/test_table1.tsv',
            '-   step_type: load-redshift',
            '    schema: dev',
            '    table: test_table',
        ])
        # Definition has no description field
        self.test_definition = {
            'name': 'example_load_redshift',
            'frequency': 'one-time',
            'description': 'Example for the load_redshift step',
            'load_time': load_time,
            'max_retries': 5,
            'steps': [{
                'step_type': 'extract-local',
                'path': 'data/test_table1.tsv',
            }, {
                'step_type': 'load-redshift',
                'schema': 'dev',
                'table': 'test_table',
            }],
        }

    @staticmethod
    @raises(ETLInputError)
    def test_yaml_extension():
        """Test if the yaml extension check works correctly
        for read_pipeline_definition
        """
        read_pipeline_definition("name.txt")

    def test_read_pipeline_definition(self):
        """Test if the pipeline definition is parsed correctly
        """
        with TempDirectory() as directory:
            directory.write('test_definition.yaml', self.test_yaml)
            result = read_pipeline_definition(
                os.path.join(directory.path, 'test_definition.yaml'))
            eq_(result, self.test_definition)

    def test_create_pipeline(self):
        """Test if simple pipeline creation is correct
        """
        result = create_pipeline(self.test_definition)
        # Check that pipeline properties are accurate
        assert result.name.endswith(self.test_definition['name'])
        eq_(result.frequency, self.test_definition['frequency'])
        eq_(result.load_hour, int(self.load_hour))
        eq_(result.load_min, int(self.load_min))
        eq_(result.max_retries, self.test_definition['max_retries'])
        # Check that vital steps are created
        steps = result.steps
        assert 'ExtractLocalStep0' in steps
        assert 'LoadRedshiftStep0' in steps
