"""Tests for create view parser
"""

from unittest import TestCase
from nose.tools import eq_
from ..create_view import parse_create_view


class TestCreateViewStatement(TestCase):
    """Tests for create view
    """
    @staticmethod
    def test_basic():
        """Basic test for create view
        """
        query = 'CREATE VIEW orders AS (' + \
                'SELECT x, y, z from xyz_table)'

        full_name = 'orders'
        replace = False

        output = parse_create_view(query)

        eq_(output['view_name'], full_name)
        eq_(output['replace'], replace)
        eq_(output['select_statement'], 'SELECT x, y, z from xyz_table')
