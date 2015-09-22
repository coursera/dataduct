"""Tests for select statement parser
"""

from nose.tools import eq_
from nose.tools import raises
from pyparsing import ParseException
from unittest import TestCase

from ..select_query import parse_column_name
from ..select_query import parse_select_columns
from ..select_query import parse_select_dependencies


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
                 ',CASE WHEN y=10 THEN 5 ELSE z AS a'
                 ',CASE WHEN x THEN COUNT(MIN(x,y)) ELSE MIN(x) END AS b'
                 ',COUNT(1) AS c'
                 ",CASE WHEN course_platform = 'spark' THEN 'v1-' "
                 "|| topic_id::VARCHAR ELSE course_id END AS course_id "
                 'FROM abc')

        result = [
            'x',
            'CASE WHEN y=10 THEN 5 ELSE z AS a',
            'CASE WHEN x THEN COUNT(MIN(x,y)) ELSE MIN(x) END AS b',
            'COUNT(1) AS c',
            "CASE WHEN course_platform = 'spark' THEN 'v1-' " +
            "|| topic_id::VARCHAR ELSE course_id END AS course_id"
        ]

        result_names = ['x', 'a', 'b', 'c', 'course_id']

        columns = parse_select_columns(query)
        eq_(columns, result)

        column_names = [parse_column_name(c) for c in columns]
        eq_(column_names, result_names)

    @staticmethod
    def test_with_query():
        """Basic test for select statement with the with query
        """
        query = ('WITH data AS (SELECT x, y FROM xy) SELECT x,y FROM data')

        columns = parse_select_columns(query)
        eq_(columns, ['x', 'y'])
