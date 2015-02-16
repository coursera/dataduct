"""Tests for the HistoryTable class
"""
from unittest import TestCase
from nose.tools import raises
from nose.tools import eq_

from ..sql.sql_script import SqlScript
from ..table import Table
from ..history_table import HistoryTable


class TestHistoryTable(TestCase):
    """Tests for the HistoryTable class
    """

    @staticmethod
    def _create_history_table(sql):
        """Helper function"""
        return HistoryTable(SqlScript(sql))

    @staticmethod
    def _create_table(sql):
        """Helper function"""
        return Table(SqlScript(sql))

    def setUp(self):
        """Setup test fixtures
        """
        self.basic_table = self._create_table(
            """CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                value VARCHAR(25)
            );""")
        self.basic_history_table = self._create_history_table(
            """CREATE TABLE test_history_table (
                effective_ts TIMESTAMP,
                expiration_ts TIMESTAMP,
                id INTEGER,
                value VARCHAR(25)
            );""")

    @raises(ValueError)
    def test_create_history_table_no_timestamps(self):
        """Tests if creating a history table with no timestamps
        returns an error
        """
        self._create_history_table('CREATE TABLE test_table ( id INTEGER );')

    def test_history_script(self):
        """Diff comparison of generated SQL script
        """
        expected_script = [
            # Create temp table
            'CREATE TEMPORARY TABLE test_table_temp ( '
                'id INTEGER,'
                'value VARCHAR(25), '
                'PRIMARY KEY( id ) '
            ')',
            # Update temp table with source table's entries
            'INSERT INTO test_table_temp (SELECT * FROM test_table)',
            # Expire updated rows
            'UPDATE test_history_table '
                'SET expiration_ts = SYSDATE - INTERVAL \'0.000001 seconds\' '
            'FROM test_table '
            'WHERE test_history_table.id = test_table.id '
            'AND ( '
                'test_history_table.value != test_table.value '
                'OR ( '
                    'test_history_table.value IS NULL '
                    'AND test_table.value IS NOT NULL '
                ') '
                'OR ( '
                    'test_history_table.value IS NOT NULL '
                    'AND test_table.value IS NULL '
                ') '
            ') '
            'AND expiration_ts = \'9999-12-31 23:59:59.999999\'',
            # Expire deleted rows
            'UPDATE test_history_table '
                'SET expiration_ts = SYSDATE - INTERVAL \'0.000001 seconds\' '
            'WHERE ( id ) NOT IN ( '
                'SELECT id '
                'FROM test_table '
            ') '
            'AND expiration_ts = \'9999-12-31 23:59:59.999999\'',
            # Delete updated rows from temp table
            'DELETE FROM test_table_temp '
            'WHERE (id) IN ('
                'SELECT DISTINCT id '
                'FROM ('
                    'SELECT id, value '
                    'FROM test_history_table '
                    'WHERE expiration_ts = \'9999-12-31 23:59:59.999999\''
                ')'
            ')',
            # Copy temp table rows into source table
            'INSERT INTO test_history_table ('
                'SELECT * FROM ('
                    'SELECT SYSDATE, '
                        '\'9999-12-31 23:59:59.999999\'::TIMESTAMP, '
                        'id, '
                        'value '
                    'FROM test_table_temp'
                ')'
            ')',
            # Drop temp table
            'DROP TABLE IF EXISTS test_table_temp CASCADE']

        actual_script = self.basic_history_table.update_history_script(
            self.basic_table)
        eq_(len(actual_script), len(expected_script))
        for actual, expected in zip(actual_script, expected_script):
            eq_(actual.sql(), expected)
