"""Tests for helper functions
"""
from unittest import TestCase
from ..helpers import stringify_credentials
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
