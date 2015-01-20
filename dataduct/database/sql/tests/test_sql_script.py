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
        query = 'SELECT \n 1;'
        result = 'SELECT 1;'

        eq_(SqlScript(query).sql(), result)

    @staticmethod
    def test_sanatization():
        """Sanatization of comments
        """
        query = 'SELECT 1 -- test connect \n;'
        result = 'SELECT 1;'

        eq_(SqlScript(query).sql(), result)

    @staticmethod
    def test_multiple_queries():
        """Raise error if multiple queries are passed
        """
        query = 'SELECT 1; SELECT 2;'
        result = 'SELECT 1;\nSELECT 2;'
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
        query = 'SELECT 1; SELECT 2;'
        result = 2
        eq_(len(SqlScript(query)), result)

    @staticmethod
    def test_append_statement():
        """Appending a statement to sql script
        """
        script = SqlScript()
        script.append(SqlStatement('SELECT 1'))
        eq_(script.sql(), 'SELECT 1;')

    @staticmethod
    def test_append_script():
        """Appending a script to sql script
        """
        script = SqlScript('SELECT 1;')
        script_new = SqlScript('SELECT 2;')
        script.append(script_new)
        eq_(script.sql(), 'SELECT 1;\nSELECT 2;')

    @staticmethod
    def test_append_string():
        """Appending a string to sql script
        """
        script = SqlScript('SELECT 1;')
        script.append('SELECT 2;')
        eq_(script.sql(), 'SELECT 1;\nSELECT 2;')

    @staticmethod
    def test_copy():
        """Copy a sql script
        """
        script = SqlScript('SELECT 1;')
        script_new = script.copy()
        eq_(script.sql(), script_new.sql())

        # Check if it was a copy or the same object
        assert_not_equal(id(script), id(script_new))

    @staticmethod
    def test_wrap_transaction():
        """Wrap the sql script in a transaction
        """
        script = SqlScript('SELECT 1;').wrap_transaction()
        result = 'BEGIN;\nSELECT 1;\nCOMMIT;'
        eq_(script.sql(), result)

    @staticmethod
    def test_paranthesis():
        """Test sql with paranthesis is sanatized correctly
        """
        script = SqlScript('CREATE TABLE test_begin (session_id INTEGER);')
        result = 'CREATE TABLE test_begin (session_id INTEGER);'
        eq_(script.sql(), result)

    @staticmethod
    def test_creates_table_success():
        """Correctly recognizes that the sql creates a table
        """
        script = SqlScript('CREATE TABLE test_begin (session_id INTEGER);')
        eq_(script.creates_table(), True)

    @staticmethod
    def test_creates_table_failure():
        """Correctly recognizes that the sql does not create a table
        """
        script = SqlScript('SELECT * FROM test_begin;')
        eq_(script.creates_table(), False)

    @staticmethod
    def test_creates_table_failure_not_first_statement():
        """Correctly recognizes that the first sql statement does not create
           a table
        """
        script = SqlScript("""
            SELECT * FROM test_begin;
            CREATE TABLE test_begin (session_id INTEGER);
        """)
        eq_(script.creates_table(), False)

    @staticmethod
    def test_creates_table_failure_bad_syntax():
        """Correctly recognizes bad syntax when creating a view
        """
        script = SqlScript(
            'CREATE TABLE test_begin AS (SELECT * FROM test_table);')
        eq_(script.creates_table(), False)

    @staticmethod
    def test_creates_view_success():
        """Correctly recognizes that the sql creates a view
        """
        script = SqlScript(
            'CREATE VIEW test_begin AS (SELECT * FROM test_table);')
        eq_(script.creates_view(), True)

    @staticmethod
    def test_creates_view_failure():
        """Correctly recognizes that the sql does not create a view
        """
        script = SqlScript('SELECT * FROM test_begin;')
        eq_(script.creates_table(), False)

    @staticmethod
    def test_creates_view_failure_not_first_statement():
        """Correctly recognizes that the first sql statment does not create
           a view
        """
        script = SqlScript("""
            SELECT * FROM test_begin;
            CREATE VIEW test_begin AS (SELECT * FROM test_table);
        """)
        eq_(script.creates_view(), False)

    @staticmethod
    def test_creates_view_failure_bad_syntax():
        """Correctly recognizes bad syntax when creating a view
        """
        script = SqlScript('CREATE VIEW test_begin (session_id INTEGER);')
        eq_(script.creates_view(), False)
