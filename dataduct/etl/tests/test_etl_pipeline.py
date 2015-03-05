"""Tests for the ETL Pipeline object
"""
import unittest
from nose.tools import raises
from nose.tools import eq_

from datetime import timedelta
from ..etl_pipeline import ETLPipeline
from ...utils.exceptions import ETLInputError


class EtlPipelineTests(unittest.TestCase):
    """Tests for the ETL Pipeline object
    """

    def setUp(self):
        """Setup text fixtures
        """
        self.default_pipeline = ETLPipeline('test_pipeline')

    @staticmethod
    def test_construct_etl_pipeline():
        """Test if the constructor for EtlPipeline is correct
        """
        result = ETLPipeline(
            'test_pipeline',
            frequency='one-time',
            ec2_resource_config={'terminate_after':'2 Hours'},
            time_delta=timedelta(seconds=3600),
            emr_cluster_config={'cfg1': 'value'},
            load_time='12:34',
            topic_arn='sns:topic-arn:test-case',
            max_retries=5,
            bootstrap={'cfg1': 'value'},
        )
        assert result.name.endswith('test_pipeline')
        eq_(result.frequency, 'one-time')
        eq_(result.ec2_resource_config, {'terminate_after':'2 Hours'})
        eq_(result.load_hour, 12)
        eq_(result.load_min, 34)
        eq_(result.time_delta, timedelta(seconds=3600))
        eq_(result.max_retries, 5)
        eq_(result.topic_arn, 'sns:topic-arn:test-case')
        eq_(result.bootstrap_definitions, {'cfg1': 'value'})
        eq_(result.emr_cluster_config, {'cfg1': 'value'})

    @staticmethod
    def test_no_load_time_default_none():
        """Test if the load_hour and load_min get set to None
        if load_time is None
        """
        result = ETLPipeline('no_load_time_pipeline', load_time=None)
        eq_(result.load_hour, None)
        eq_(result.load_min, None)

    @raises(ETLInputError)
    def test_bad_data_type_throws(self):
        """Test that exception is thrown if the data_type parameter for
        _s3_uri is bad
        """
        self.default_pipeline._s3_uri('TEST_DATA_TYPE')
