"""Tests for the SqlScript class
"""
from unittest import TestCase
from nose.tools import eq_
from nose.tools import assert_not_equal

from ..sql_statement import SqlStatement
from ..sql_script import SqlScript


class TestSqlScript(TestCase):
    """Tests for sql Script function
    """
    @staticmethod
    def test_basic():
        """Basic test for Script declaration
        """
        query = 'select \n 1;'
        result = 'select 1;'

        eq_(SqlScript(query).sql(), result)

    @staticmethod
    def test_sanatization():
        """Sanatization of comments
        """
        query = 'select 1 -- test connect \n;'
        result = 'select 1;'

        eq_(SqlScript(query).sql(), result)

    @staticmethod
    def test_multiple_queries():
        """Raise error if multiple queries are passed
        """
        query = 'select 1; select 2;'
        result = 'select 1;\nselect 2;'
        eq_(SqlScript(query).sql(), result)

    @staticmethod
    def test_empty_declaration():
        """Empty if no sql query is passed
        """
        eq_(SqlScript().sql(), ';')

    @staticmethod
    def test_length():
        """Length of sql script
        """
        query = 'select 1; select 2;'
        result = 2
        eq_(len(SqlScript(query)), result)

    @staticmethod
    def test_append_statement():
        """Appending a statement to sql script
        """
        script = SqlScript()
        script.append(SqlStatement('Select 1'))
        eq_(script.sql(), 'Select 1;')

    @staticmethod
    def test_append_script():
        """Appending a script to sql script
        """
        script = SqlScript('Select 1;')
        script_new = SqlScript('Select 2;')
        script.append(script_new)
        eq_(script.sql(), 'Select 1;\nSelect 2;')

    @staticmethod
    def test_append_string():
        """Appending a string to sql script
        """
        script = SqlScript('Select 1;')
        script.append('Select 2;')
        eq_(script.sql(), 'Select 1;\nSelect 2;')

    @staticmethod
    def test_copy():
        """Copy a sql script
        """
        script = SqlScript('Select 1;')
        script_new = script.copy()
        eq_(script.sql(), script_new.sql())

        # Check if it was a copy or the same object
        assert_not_equal(id(script), id(script_new))

    @staticmethod
    def test_wrap_transaction():
        """Wrap the sql script in a transaction
        """
        script = SqlScript('Select 1;').wrap_transaction()
        result = 'BEGIN;\nSelect 1;\nCOMMIT;'
        eq_(script.sql(), result)

    @staticmethod
    def test_paranthesis():
        """Test sql with paranthesis is sanatized correctly
        """
        script = SqlScript('create table test (session_id INTEGER);')
        result = 'create table test (session_id INTEGER);'
        eq_(script.sql(), result)
