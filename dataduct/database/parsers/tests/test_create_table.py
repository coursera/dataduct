"""Tests for create table parser
"""

from unittest import TestCase
from nose.tools import eq_
from ..create_table import parse_create_table


class TestCreateTableStatement(TestCase):
    """Tests for create table
    """
    @staticmethod
    def test_basic():
        """Basic test for create table
        """
        query = 'CREATE TABLE orders (' +\
                'customer_id INTEGER DISTKEY PRIMARY KEY,' +\
                'customer_name VARCHAR(200))'

        full_name = 'orders'
        temporary = False
        exists_checks = False

        output = parse_create_table(query)

        eq_(output['full_name'], full_name)
        eq_(output['temporary'], temporary)
        eq_(output['exists_checks'], exists_checks)
        eq_(len(output['constraints']), 0)
        eq_(len(output['columns']), 2)
