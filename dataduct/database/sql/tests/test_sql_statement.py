"""Tests for the SqlStatement class
"""
from unittest import TestCase
from nose.tools import eq_
from nose.tools import raises

from ..sql_statement import SqlStatement


class TestSqlStatement(TestCase):
    """Tests for sql statement function
    """
    @staticmethod
    def test_basic():
        """Basic test for statement declaration
        """
        query = 'select \n 1;'
        result = 'select 1'

        eq_(SqlStatement(query).sql(), result)

    @staticmethod
    def test_sanatization():
        """Sanatization of comments
        """
        query = 'select 1 -- test connect \n;'
        result = 'select 1'

        eq_(SqlStatement(query).sql(), result)

    @staticmethod
    @raises(ValueError)
    def test_error():
        """Raise error if multiple queries are passed
        """
        query = 'select 1; select 2;'
        SqlStatement(query)

    @staticmethod
    def test_empty_declaration():
        """Empty if no sql query is passed
        """
        eq_(SqlStatement().sql(), '')
