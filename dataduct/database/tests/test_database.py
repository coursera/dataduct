"""Tests for Database
"""
import os

from unittest import TestCase
from testfixtures import TempDirectory
from nose.tools import assert_not_equal
from nose.tools import eq_

from ..database import Database
from ..table import Table
from ..view import View
from ..sql import SqlScript


class TestDatabase(TestCase):
    """Tests for Database
    """

    @staticmethod
    def _create_table(sql):
        """Creates a table object from a SQL string
        """
        return Table(SqlScript(sql))

    @staticmethod
    def _create_view(sql):
        """Creates a view object from a SQL string
        """
        return View(SqlScript(sql))

    def test_create(self):
        """Tests database initialization
        """
        table = self._create_table('CREATE TABLE test_begin (id INTEGER);')
        database = Database(relations=[table])

        # Verify that the database is constructed properly
        eq_(database.num_tables, 1)
        eq_(database.num_views, 0)
        assert_not_equal(database.relation('test_begin'), None)

    @staticmethod
    def test_create_from_file():
        """Tests database initialization from file
        """
        with TempDirectory() as d:
            # Create files in the temp directory
            d.write('test_table.sql',
                    'CREATE TABLE test_table (session_id INTEGER);')
            d.write('test_view.sql',
                    'CREATE VIEW test_view AS (SELECT * FROM test_table);')
            database = Database(files=[os.path.join(d.path, 'test_table.sql'),
                                       os.path.join(d.path, 'test_view.sql')])

            # Verify that the database is constructed properly
            eq_(database.num_tables, 1)
            eq_(database.num_views, 1)
            assert_not_equal(database.relation('test_table'), None)
            assert_not_equal(database.relation('test_view'), None)

    @staticmethod
    def test_create_from_file_no_relation():
        """Database initialization with a file that does not create a
        relation
        """
        with TempDirectory() as d:
            # Create a file in the temp directory
            d.write('test.sql',
                    'SELECT * FROM test_table;')
            try:
                Database(files=[os.path.join(d.path, 'test.sql')])
                assert False
            except ValueError:
                pass

    @staticmethod
    def test_create_two_arguments():
        """Must create database with less than two arguments
        """
        try:
            Database(relations=['test_rel'], files=['test_file'])
            assert False
        except ValueError:
            pass

    def test_create_duplicate_relations(self):
        """Database initalization with duplicate relations
        """
        table = self._create_table(
            'CREATE TABLE test_begin (session_id INTEGER);')
        try:
            Database(relations=[table, table])
            assert False
        except ValueError:
            pass

    def test_database_copy(self):
        """Copying a database is a deepcopy
        """
        table = self._create_table(
            'CREATE TABLE test_begin (session_id INTEGER);')
        database = Database(relations=[table])
        database_copy = database.copy()

        # Check that the copied database contains the relation
        assert_not_equal(database_copy.relation('test_begin'), None)

        # Delete the relation in the copy
        database_copy._relations = {}

        # Check that the original database still contains the relation
        assert_not_equal(database.relation('test_begin'), None)

    def test_database_has_cycles(self):
        """Check if a database has cycles
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER REFERENCES first_table(id1),
                id2 INTEGER
            );""")

        database = Database(relations=[first_table, second_table])
        eq_(database.has_cycles(), True)

    def test_database_has_no_cycles(self):
        """Check if a database has no cycles
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER,
                id2 INTEGER
            );""")

        database = Database(relations=[first_table, second_table])
        eq_(database.has_cycles(), False)

    def test_database_has_no_cycles_2(self):
        """Check if a database has no cycles
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER REFERENCES first_table(id1),
                id2 INTEGER
            );""")

        database = Database(relations=[first_table, second_table])
        eq_(database.has_cycles(), False)

    def test_database_sorted_relations(self):
        """Get the topological sort of the database
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER,
                id2 INTEGER
            );""")

        database = Database(relations=[first_table, second_table])
        relations = database.sorted_relations()

        # Verify that the relations are sorted correctly
        eq_(len(relations), 2)
        eq_(relations[0].table_name, 'second_table')
        eq_(relations[1].table_name, 'first_table')

    def test_database_sorted_relations_cyclic(self):
        """Get the topological sort of the database with cycles
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER REFERENCES first_table(id1),
                id2 INTEGER
            );""")

        database = Database(relations=[first_table, second_table])
        try:
            database.sorted_relations()
            assert False
        except RuntimeError:
            pass

    def _test_database_scripts(self, function_name, expected_sql, **kwargs):
        """Generate SQL scripts with a preset database
        """
        table = self._create_table('CREATE TABLE test_table ( id INTEGER );')
        view = self._create_view("""CREATE VIEW test_view AS (
                                         SELECT id FROM test_table
                                     );""")
        database = Database(relations=[table, view])
        func = getattr(database, function_name)
        eq_(func(**kwargs).sql(), expected_sql)

    def test_database_create_relations_script(self):
        """Creating relations in the database
        """
        self._test_database_scripts(
            'create_relations_script',
            'CREATE TABLE test_table ( id INTEGER );\n'
            'CREATE VIEW test_view AS ( SELECT id FROM test_table );')

    def test_database_drop_relations_script(self):
        """Dropping relations in the database
        """
        self._test_database_scripts(
            'drop_relations_script',
            'DROP TABLE IF EXISTS test_table CASCADE;\n'
            'DROP VIEW IF EXISTS test_view CASCADE;')

    def test_database_recreate_relations_script(self):
        """Recreating relations in the database
        """
        self._test_database_scripts(
            'recreate_relations_script',
            'DROP TABLE IF EXISTS test_table CASCADE;\n'
            'CREATE TABLE test_table ( id INTEGER );\n'
            'DROP VIEW IF EXISTS test_view CASCADE;\n'
            'CREATE VIEW test_view AS ( SELECT id FROM test_table );')

    def test_database_recreate_table_dependencies(self):
        """Recreating table dependencies
        """
        first_table = self._create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        second_table = self._create_table(
            """CREATE TABLE second_table (
                id1 INTEGER,
                id2 INTEGER
            );""")
        view = self._create_view(
            """CREATE VIEW view AS (
                SELECT id1 FROM second_table
            );""")
        database = Database(relations=[first_table, second_table, view])

        eq_(database.recreate_table_dependencies('second_table').sql(),
            'ALTER TABLE first_table ADD FOREIGN KEY (id2) '
            'REFERENCES second_table (id2);\n'
            'DROP VIEW IF EXISTS view CASCADE;\n'
            'CREATE VIEW view AS ( SELECT id1 FROM second_table );')
        eq_(database.recreate_table_dependencies('first_table').sql(), ';')
