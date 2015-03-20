"""Tests the utils functions
"""
from unittest import TestCase
from nose.tools import eq_

from ..utils import balanced_parenthesis
from ..utils import sanitize_sql


class TestSqlUtils(TestCase):
    """Tests for sql utils function
    """
    @staticmethod
    def test_balanced_paranthesis():
        """Test for balanced_parenthesis
        """
        eq_(balanced_parenthesis('SELECT 1;'), True)
        eq_(balanced_parenthesis('SELECT 1(;'), False)
        eq_(balanced_parenthesis('SELECT 1();'), True)
        eq_(balanced_parenthesis('SELECT 1(abcd);'), True)
        eq_(balanced_parenthesis('SELECT 1(ab[cd);'), True)
        eq_(balanced_parenthesis('SELECT 1(ab[cd));'), False)
        eq_(balanced_parenthesis('SELECT 1);'), False)
        eq_(balanced_parenthesis('SELECT 1(ab)(ab);'), True)
        eq_(balanced_parenthesis('SELECT 1(a(ab)b);'), True)

    @staticmethod
    def test_sanitize_sql():
        """Test for sanitize_sql
        """
        sql = "SELECT 1 if x='x;y'; SELECT 1 ;"
        eq_(sanitize_sql(sql), ["SELECT 1 if x='x;y'", 'SELECT 1'])
