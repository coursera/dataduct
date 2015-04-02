"""Tests for Relation
"""
from unittest import TestCase

from .helpers import create_table
from .helpers import create_view
from .helpers import compare_scripts


class TestRelation(TestCase):
    """Tests for Relation (tables or views)
    """

    def setUp(self):
        """Setup test fixtures for the database tests
        """
        # A basic table and view
        self.basic_table = create_table(
            'CREATE TABLE test_table (id INTEGER);')
        self.basic_view = create_view(
            'CREATE VIEW test_view AS (SELECT * FROM test_table);')

    def test_unload_script(self):
        """Tests if the unload script generates successfully
        """
        result = [
            ("UNLOAD ('SELECT * FROM test_table;') TO 's3://test/' "
             "CREDENTIALS 'aws_access_key_id=a;aws_secret_access_key=b' "
             "DELIMITER '\t' ESCAPE NULL AS 'NULL'")
        ]
        compare_scripts(
            self.basic_table.unload_script('s3://test/', 'a', 'b'),
            result)

    def test_unload_script_with_token(self):
        """Tests if the unload script generates successfully
        """
        result = [
            ("UNLOAD ('SELECT * FROM test_view;') TO 's3://test/' "
             "CREDENTIALS "
             "'aws_access_key_id=a;aws_secret_access_key=b;token=c' "
             "DELIMITER '\t' ESCAPE NULL AS 'NULL'")
        ]
        compare_scripts(
            self.basic_view.unload_script('s3://test/', 'a', 'b', 'c'),
            result)

    def test_load_script(self):
        """Tests if the unload script generates successfully
        """
        result = [
            ("COPY test_table FROM 's3://test/' "
             "CREDENTIALS 'aws_access_key_id=a;aws_secret_access_key=b' "
             "DELIMITER '\t' ESCAPE NULL AS 'NULL'")
        ]
        compare_scripts(
            self.basic_table.load_script('s3://test/', 'a', 'b'),
            result)

    def test_load_script_with_token(self):
        """Tests if the unload script generates successfully
        """
        result = [
            ("COPY test_view FROM 's3://test/' "
             "CREDENTIALS "
             "'aws_access_key_id=a;aws_secret_access_key=b;token=c' "
             "DELIMITER '\t' ESCAPE NULL AS 'NULL'")
        ]
        compare_scripts(
            self.basic_view.load_script('s3://test/', 'a', 'b', 'c'),
            result)
