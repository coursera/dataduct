"""Tests for Database
"""
import os

from unittest import TestCase
from testfixtures import TempDirectory
from nose.tools import assert_not_equal
from nose.tools import eq_
from nose.tools import raises

from ..database import Database
from .helpers import create_table
from .helpers import create_view
from .helpers import compare_scripts


class TestDatabase(TestCase):
    """Tests for Database
    """

    def setUp(self):
        """Setup test fixtures for the database tests
        """
        # A basic table and view
        self.basic_table = create_table(
            'CREATE TABLE test_table (id INTEGER);')
        self.basic_view = create_view(
            'CREATE VIEW test_view AS (SELECT * FROM test_table);')

        # Create tables with dependencies between them
        self.first_table = create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER
            );""")
        self.first_table_dependent = create_table(
            """CREATE TABLE first_table (
                id1 INTEGER,
                id2 INTEGER REFERENCES second_table(id2)
            );""")
        self.second_table = create_table(
            """CREATE TABLE second_table (
                id1 INTEGER,
                id2 INTEGER
            );""")
        self.second_table_dependent = create_table(
            """CREATE TABLE second_table (
                id1 INTEGER REFERENCES first_table(id1),
                id2 INTEGER
            );""")

        # Create a template database to test script generation
        table = create_table('CREATE TABLE test_table ( id INTEGER );')
        view = create_view("""CREATE VIEW test_view AS (
                               SELECT id FROM test_table
                           );""")
        self.script_database = Database(relations=[table, view])

    def test_create(self):
        """Tests database initialization
        """
        database = Database(relations=[self.basic_table])

        # Verify that the database is constructed properly
        eq_(database.num_tables, 1)
        eq_(database.num_views, 0)
        assert_not_equal(database.relation(self.basic_table.full_name), None)

    def test_create_from_file(self):
        """Tests database initialization from file
        """
        with TempDirectory() as d:
            # Create files in the temp directory
            d.write(self.basic_table.full_name,
                    self.basic_table.sql_statement.sql())
            d.write(self.basic_view.full_name,
                    self.basic_view.sql_statement.sql())
            database = Database(
                files=[os.path.join(d.path, self.basic_table.full_name),
                       os.path.join(d.path, self.basic_view.full_name)])

            # Verify that the database is constructed properly
            eq_(database.num_tables, 1)
            eq_(database.num_views, 1)
            assert_not_equal(
                database.relation(self.basic_table.full_name), None)
            assert_not_equal(
                database.relation(self.basic_view.full_name), None)

    @staticmethod
    @raises(ValueError)
    def test_create_from_file_no_relation():
        """Database initialization with a file that does not create a
        relation
        """
        with TempDirectory() as d:
            # Create a file in the temp directory
            d.write('test.sql',
                    'SELECT * FROM test_table;')
            Database(files=[os.path.join(d.path, 'test.sql')])

    @staticmethod
    @raises(ValueError)
    def test_create_two_arguments():
        """Must create database with less than two arguments
        """
        Database(relations=['test_rel'], files=['test_file'])

    @raises(ValueError)
    def test_create_duplicate_relations(self):
        """Database initialization with duplicate relations
        """
        Database(relations=[self.basic_table, self.basic_table])

    def test_database_copy(self):
        """Copying a database is a deepcopy
        """
        database = Database(relations=[self.basic_table])
        database_copy = database.copy()

        # Check that the copied database contains the relation
        assert_not_equal(
            database_copy.relation(self.basic_table.full_name), None)

        # Delete the relation in the copy
        database_copy._relations = {}

        # Check that the original database still contains the relation
        assert_not_equal(
            database.relation(self.basic_table.full_name), None)

    def test_database_has_cycles(self):
        """Check if a database has cycles
        """
        database = Database(relations=[self.first_table_dependent,
                                       self.second_table_dependent])
        eq_(database.has_cycles(), True)

    def test_database_has_no_cycles(self):
        """Check if a database has no cycles
        """
        database = Database(relations=[self.first_table_dependent,
                                       self.second_table])
        eq_(database.has_cycles(), False)

    def test_database_has_no_cycles_2(self):
        """Check if a database has no cycles
        """
        database = Database(relations=[self.first_table,
                                       self.second_table_dependent])
        eq_(database.has_cycles(), False)

    def test_database_sorted_relations(self):
        """Get the topological sort of the database
        """
        database = Database(relations=[self.first_table_dependent,
                                       self.second_table])
        relations = database.sorted_relations()

        # Verify that the relations are sorted correctly
        eq_(len(relations), 2)
        eq_(relations[0].table_name, self.second_table.full_name)
        eq_(relations[1].table_name, self.first_table_dependent.full_name)

    @raises(RuntimeError)
    def test_database_sorted_relations_cyclic(self):
        """Get the topological sort of the database with cycles
        """
        database = Database(relations=[self.first_table_dependent,
                                       self.second_table_dependent])
        database.sorted_relations()

    def test_database_create_relations_script(self):
        """Creating relations in the database
        """
        result = ['CREATE TABLE test_table ( id INTEGER )',
                  'CREATE VIEW test_view AS ( SELECT id FROM test_table )']
        compare_scripts(
            self.script_database.create_relations_script(False),
            result)

    def test_database_drop_relations_script(self):
        """Dropping relations in the database
        """
        result = ['DROP TABLE IF EXISTS test_table CASCADE',
                  'DROP VIEW IF EXISTS test_view CASCADE']
        compare_scripts(
            self.script_database.drop_relations_script(),
            result)

    def test_database_recreate_relations_script(self):
        """Recreating relations in the database
        """
        result = ['DROP TABLE IF EXISTS test_table CASCADE',
                  'CREATE TABLE test_table ( id INTEGER )',
                  'DROP VIEW IF EXISTS test_view CASCADE',
                  'CREATE VIEW test_view AS ( SELECT id FROM test_table )']
        compare_scripts(
            self.script_database.recreate_relations_script(False),
            result)

    def test_database_recreate_table_dependencies(self):
        """Recreating table dependencies
        """
        view = create_view(
            """CREATE VIEW view AS (
                SELECT id1 FROM second_table
            );""")
        database = Database(relations=[self.first_table_dependent,
                                       self.second_table, view])

        result = ['ALTER TABLE first_table ADD FOREIGN KEY (id2) '
                  'REFERENCES second_table (id2)',
                  'DROP VIEW IF EXISTS view CASCADE',
                  'CREATE VIEW view AS ( SELECT id1 FROM second_table )']
        compare_scripts(
            database.recreate_table_dependencies('second_table', False),
            result)
        eq_(database.recreate_table_dependencies('first_table', False).sql(),
            ';')
