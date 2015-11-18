"""Tests for the column parser in the create_table file
"""
from unittest import TestCase
from nose.tools import eq_

from ..create_table import get_column_parser


class TestCreateTableStatement(TestCase):
    """Tests for the column parser
    """
    @staticmethod
    def test_basic():
        """Tests the parser can extract data about a basic column
        """
        result = get_column_parser().parseString("test_column INTEGER")
        eq_(result['column_name'], 'test_column')
        eq_(result['column_type'], 'INTEGER')

    @staticmethod
    def test_smallint():
        """Parser can parse SMALLINT columns and its aliases
        """
        result = get_column_parser().parseString("test_column SMALLINT")
        eq_(result['column_type'], 'SMALLINT')

        result = get_column_parser().parseString("test_column INT2")
        eq_(result['column_type'], 'INT2')

    @staticmethod
    def test_integer():
        """Parser can parse INTEGER columns and its aliases
        """
        result = get_column_parser().parseString("test_column INTEGER")
        eq_(result['column_type'], 'INTEGER')

        result = get_column_parser().parseString("test_column INT")
        eq_(result['column_type'], 'INT')

        result = get_column_parser().parseString("test_column INT4")
        eq_(result['column_type'], 'INT4')

    @staticmethod
    def test_bigint():
        """Parser can parse BIGINT and its aliases
        """
        result = get_column_parser().parseString("test_column BIGINT")
        eq_(result['column_type'], 'BIGINT')

        result = get_column_parser().parseString("test_column INT8")
        eq_(result['column_type'], 'INT8')

    @staticmethod
    def test_decimal():
        """Parser can parse DECIMAL and its aliases
        """
        result = get_column_parser().parseString("test_column DECIMAL(1,1)")
        eq_(result['column_type'], 'DECIMAL(1,1)')

        result = get_column_parser().parseString("test_column NUMERIC(1, 1)")
        eq_(result['column_type'], 'NUMERIC(1, 1)')

    @staticmethod
    def test_real():
        """Parser can parse REAL and its aliases
        """
        result = get_column_parser().parseString("test_column REAL")
        eq_(result['column_type'], 'REAL')

        result = get_column_parser().parseString("test_column FLOAT4")
        eq_(result['column_type'], 'FLOAT4')

    @staticmethod
    def test_double():
        """Parser can parse DOUBLE and its aliases
        """
        result = get_column_parser().parseString("test_column DOUBLE")
        eq_(result['column_type'], 'DOUBLE')

        result = get_column_parser().parseString("test_column DOUBLE PRECISION")
        eq_(result['column_type'], 'DOUBLE PRECISION')

        result = get_column_parser().parseString("test_column FLOAT8")
        eq_(result['column_type'], 'FLOAT8')

        result = get_column_parser().parseString("test_column FLOAT")
        eq_(result['column_type'], 'FLOAT')

    @staticmethod
    def test_char():
        """Parser can parse CHAR and its aliases
        """
        result = get_column_parser().parseString("test_column CHAR")
        eq_(result['column_type'], 'CHAR')

        result = get_column_parser().parseString("test_column CHARACTER")
        eq_(result['column_type'], 'CHARACTER')

        result = get_column_parser().parseString("test_column NCHAR")
        eq_(result['column_type'], 'NCHAR')

        result = get_column_parser().parseString("test_column BPCHAR")
        eq_(result['column_type'], 'BPCHAR')

    @staticmethod
    def test_varchar():
        """Parser can parse CHAR and its aliases
        """
        result = get_column_parser().parseString("test_column VARCHAR(1)")
        eq_(result['column_type'], 'VARCHAR(1)')

        result = get_column_parser().parseString("test_column TEXT(1)")
        eq_(result['column_type'], 'TEXT(1)')

        result = get_column_parser().parseString("test_column NVARCHAR(1)")
        eq_(result['column_type'], 'NVARCHAR(1)')

    @staticmethod
    def test_date():
        """Parser can parse DATE
        """
        result = get_column_parser().parseString("test_column DATE")
        eq_(result['column_type'], 'DATE')

    @staticmethod
    def test_timestamp():
        """Parser can parse TIMESTAMP
        """
        result = get_column_parser().parseString("test_column TIMESTAMP")
        eq_(result['column_type'], 'TIMESTAMP')

    @staticmethod
    def test_not_null():
        """Parser can parse NOT NULL columns
        """
        result = get_column_parser().parseString("test_column INT NOT NULL")
        eq_(result['is_not_null'], True)

    @staticmethod
    def test_is_null():
        """Parser can parse NULL columns
        """
        result = get_column_parser().parseString("test_column INT NULL")
        eq_(result['is_null'], True)

    @staticmethod
    def test_is_primarykey():
        """Parser can parse PRIMARY KEY columns
        """
        result = get_column_parser().parseString("test_column INT PRIMARY KEY")
        eq_(result['is_primarykey'], True)

    @staticmethod
    def test_is_distkey():
        """Parser can parse DISTKEY columns
        """
        result = get_column_parser().parseString("test_column INT DISTKEY")
        eq_(result['is_distkey'], True)

    @staticmethod
    def test_is_sortkey():
        """Parser can parse SORTKEY columns
        """
        result = get_column_parser().parseString("test_column INT SORTKEY")
        eq_(result['is_sortkey'], True)

    @staticmethod
    def test_foreign_key_reference():
        """Parser can parse FK columns
        """
        sql = "test_column INT REFERENCES test_table(test_column2)"
        result = get_column_parser().parseString(sql)
        eq_(result['fk_table'], 'test_table')
        eq_(result['fk_reference'][0], 'test_column2')

    @staticmethod
    def test_encoding():
        """Parser can parse encoded columns
        """
        sql = "test_column INT ENCODE anything"
        result = get_column_parser().parseString(sql)
        eq_(result['encoding'], 'anything')
