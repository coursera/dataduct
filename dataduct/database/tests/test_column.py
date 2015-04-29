"""Tests for the Column class
"""
from unittest import TestCase
from nose.tools import eq_
from nose.tools import raises

from ..column import Column


class TestColumn(TestCase):
    """Tests for the Column class
    """
    @staticmethod
    def test_column_construction():
        """Tests that the constructor for the column class is working
        """
        column = Column(
            column_name='test',
            column_type='INTEGER',
            encoding='lzo',
            fk_reference='test_fk_column',
            fk_table='test_fk_table',
            is_distkey=True,
            is_sortkey=True,
            is_primarykey=True,
            is_null=False,
            is_not_null=True,
            position=1,
        )
        eq_(column.column_name, 'test')
        eq_(column.name, 'test')
        eq_(column.column_type, 'INTEGER')
        eq_(column.encoding, 'lzo')
        eq_(column.fk_reference, 'test_fk_column')
        eq_(column.fk_table, 'test_fk_table')
        eq_(column.is_distkey, True)
        eq_(column.is_sortkey, True)
        eq_(column.is_primarykey, True)
        eq_(column.is_null, False)
        eq_(column.is_not_null, True)
        eq_(column.position, 1)

    @staticmethod
    @raises(ValueError)
    def test_column_both_null_and_not_null_raises():
        """Tests that an exception is raised if the column is constructed as
        both null and not null
        """
        Column(
            column_name='test',
            column_type='INTEGER',
            is_null=True,
            is_not_null=True
        )

    @staticmethod
    def test_column_primary_key_null_and_not_null_set():
        """Tests that null and not null flags are set if the column is a
        primary key
        """
        column = Column(
            column_name='test',
            column_type='INTEGER',
            is_primarykey=True,
            is_null=True,
            is_not_null=False
        )
        eq_(column.is_null, False)
        eq_(column.is_not_null, True)

    @staticmethod
    def test_column_can_set_primary():
        """Tests that setting a column to primary works as intended
        """
        column = Column(
            column_name='test',
            column_type='INTEGER',
            is_null=True,
            is_not_null=False
        )
        eq_(column.primary, False)
        eq_(column.is_null, True)
        eq_(column.is_not_null, False)
        column.primary = True
        eq_(column.primary, True)
        eq_(column.is_null, False)
        eq_(column.is_not_null, True)

    @staticmethod
    def test_column_to_string_no_type():
        """Tests that serializing a Column to a string works as intended
        when there is no column type
        """
        column = Column('test', None)
        eq_(str(column), 'test')

    @staticmethod
    def test_column_to_string_with_type():
        """Tests that serializing a Column to a string works as intended
        when there is a column type
        """
        column = Column('test', 'INTEGER')
        eq_(str(column), 'test INTEGER')
