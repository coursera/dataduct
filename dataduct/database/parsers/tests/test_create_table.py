"""Tests for create table parser
"""

from unittest import TestCase
from nose.tools import eq_
from nose.tools import raises
from pyparsing import ParseException

from ..create_table import parse_create_table
from ..create_table import create_exists_clone


class TestCreateTableStatement(TestCase):
    """Tests for create table
    """
    @staticmethod
    def test_basic():
        """Basic test for create table
        """
        query = ('CREATE TABLE orders ('
                 'customer_id INTEGER DISTKEY PRIMARY KEY,'
                 'customer_name VARCHAR(200))')

        output = parse_create_table(query)

        eq_(output['full_name'], 'orders')
        eq_(output['temporary'], False)
        eq_(output['exists_checks'], False)
        eq_(len(output['constraints']), 0)
        eq_(len(output['columns']), 2)

    @staticmethod
    def test_exists_clone():
        """Basic test for create table clone with exists condition
        """
        query = ('CREATE TABLE orders ('
                 'customer_id INTEGER DISTKEY PRIMARY KEY,'
                 'customer_name VARCHAR(200))')

        exists_clone = create_exists_clone(query)
        output = parse_create_table(exists_clone)
        eq_(output['full_name'], 'orders')
        eq_(output['temporary'], False)
        eq_(output['exists_checks'], True)

    @staticmethod
    @raises(ParseException)
    def test_bad_input():
        """Feeding malformed input into create table
        """
        query = 'CREATE TABLE orders (' +\
                'customer_id INTEGER DISTKEY PRIMARY KEY'
        parse_create_table(query)

    @staticmethod
    @raises(ParseException)
    def test_bad_input_in_columns():
        """Feeding malformed input into create table
        """
        query = 'CREATE TABLE orders (' +\
                'customer_id NEGATIVE DISTKEY PRIMARY KEY)'
        parse_create_table(query)
