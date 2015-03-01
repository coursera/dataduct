"""Tests for select statement parser
"""

from unittest import TestCase
from nose.tools import eq_
from nose.tools import raises
from pyparsing import ParseException

from ..select_query import parse_select_dependencies
from ..select_query import parse_select_columns
from ..select_query import parse_column_name


class TestCreateTableStatement(TestCase):
    """Tests for create table
    """
    @staticmethod
    def test_basic():
        """Basic test for select statement
        """
        query = ('SELECT x, y, z AS t FROM abc JOIN pqr USING(y) WHERE x=1')

        dependencies = parse_select_dependencies(query)
        eq_(dependencies, ['abc', 'pqr'])

        columns = parse_select_columns(query)
        eq_(columns, ['x', 'y', 'z AS t'])

        column_name = parse_column_name(columns[0])
        eq_(column_name, 'x')

        column_name = parse_column_name(columns[2])
        eq_(column_name, 't')

    @staticmethod
    @raises(ParseException)
    def test_bad_input():
        """Feeding malformed input into create table
        """
        query = 'SELECT x, y, z'
        parse_select_dependencies(query)

    @staticmethod
    def test_columns():
        """Basic test for select statement
        """
        query = ('SELECT x'
                 ',CASE WHEN y=10 THEN 5 ELSE z'
                 ',CASE WHEN x THEN COUNT(MIN(x,y)) ELSE MIN(x) END'
                 ',COUNT(1) AS c '
                 'FROM abc')

        result = [
            'x',
            'CASE WHEN y=10 THEN 5 ELSE z',
            'CASE WHEN x THEN COUNT(MIN(x,y)) ELSE MIN(x) END',
            'COUNT(1) AS c',
        ]

        columns = parse_select_columns(query)
        eq_(columns, result)
