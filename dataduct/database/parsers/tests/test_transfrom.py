"""Tests for the transformation steps
"""
from unittest import TestCase
from nose.tools import eq_

from ..transform import split_statements
from ..transform import remove_comments
from ..transform import remove_empty_statements
from ..transform import remove_transactional
from ..transform import remove_newlines


class TestRemoveEmptyStatements(TestCase):
    """Tests for remove_empty_statements function
    """
    @staticmethod
    def test_basic():
        """Basic test for single location of seperator
        """
        data = 'a;;;'
        result = 'a;'

        eq_(remove_empty_statements(data), result)

    @staticmethod
    def test_multiple_statements_single_duplication():
        """Test for multiple locations of seperator with single duplication
        """
        data = 'a; b;; c;'
        result = 'a; b; c;'

        eq_(remove_empty_statements(data), result)

    @staticmethod
    def test_multiple_statements_multiple_duplication():
        """Test for multiple locations of seperator with multiple duplication
        """
        data = 'a;;; b;; c;;;'
        result = 'a; b; c;'

        eq_(remove_empty_statements(data), result)

    @staticmethod
    def test_start_empty():
        """Test for removing an empty statement at start
        """
        data = '; a;  ; ;;; b; c;;;'
        result = ' a; b; c;'

        eq_(remove_empty_statements(data), result)


class TestRemoveNewLines(TestCase):
    """Tests for remove_empty_statements function
    """
    @staticmethod
    def test_basic():
        """Basic test for single location of seperator
        """
        data = 'a\n \n;'
        result = 'a ;'

        eq_(remove_newlines(data), result)

    @staticmethod
    def test_advanced():
        """Basic test for single location of seperator
        """
        data = 'a,\nb,\nc\n\rfrom \r\n xyz'
        result = 'a, b, c from xyz'

        eq_(remove_newlines(data), result)

    @staticmethod
    def test_quoted_newlines():
        """Basic test for single location of seperator
        """
        data = "a,\nb,\nc\n\rfrom \r\n xyz where b='a\nc'"
        result = "a, b, c from xyz where b='a\nc'"

        eq_(remove_newlines(data), result)


class TestRemoveComments(TestCase):
    """Tests for remove_comments function
    """
    @staticmethod
    def test_multiline_comment():
        """Basic test for removing multiline comments
        """
        data = 'a; /* This is \n \n a multiline comment */ b;'
        result = 'a;  b;'

        eq_(remove_comments(data), result)

    @staticmethod
    def test_singleline_comment_basic():
        """Basic test for removing singleline comments
        """
        data = 'a; b; --Comment'
        result = 'a; b; '

        eq_(remove_comments(data), result)

    @staticmethod
    def test_singleline_comment_advanced():
        """Advanced test for removing singleline comments
        """
        data = '-- Comment \n a; b;'
        result = '\n a; b;'

        eq_(remove_comments(data), result)

    @staticmethod
    def test_singleline_multiline_comment():
        """Advanced test for removing singleline comments
        """
        data = 'a; /* This is \n \n a multiline comment */ b;-- Comment '
        result = 'a;  b;'

        eq_(remove_comments(data), result)


class TestRemoveTransactional(TestCase):
    """Tests for remove_transactional function
    """
    @staticmethod
    def test_remove_none():
        """Basic test for removing nothing
        """
        data = 'a; b;'
        result = 'a; b;'

        eq_(remove_transactional(data), result)

    @staticmethod
    def test_remove_begin():
        """Basic test for removing begin
        """
        data = 'begin; a; b;'
        result = ' a; b;'

        eq_(remove_empty_statements(remove_transactional(data)), result)

    @staticmethod
    def test_remove_commit():
        """Basic test for removing commit
        """
        data = 'a; b; commit;'
        result = 'a; b;'

        eq_(remove_empty_statements(remove_transactional(data)), result)

    @staticmethod
    def test_remove_begin_commit():
        """Basic test for removing begin & commit
        """
        data = 'begin; a; b; commit;'
        result = ' a; b;'

        eq_(remove_empty_statements(remove_transactional(data)), result)

    @staticmethod
    def test_just_begin_commit():
        """Basic test for removing begin & commit
        """
        data = 'begin; commit;'
        result = ''

        eq_(remove_empty_statements(remove_transactional(data)), result)

class TestSplitOmitQuoted(TestCase):
    """Tests for split_statements function
    """
    @staticmethod
    def test_basic():
        """Basic test for spliting a string based on the seperator
        """
        data = 'a; b \n t; c;    d ; '
        result = ['a', 'b \n t', 'c', 'd']

        eq_(split_statements(data), result)

    @staticmethod
    def test_newline_sql():
        """Split SQL statement with newlines
        """
        data = 'a; b \n e; c; \n  \n  d ; '
        result = ['a', 'b \n e', 'c', 'd']

        eq_(split_statements(data), result)

    @staticmethod
    def test_paran_sql():
        """Split SQL statement with paranthesis
        """
        data = 'a; b (x\n,y,z) d; c; \n  \n  d ; '
        result = ['a', 'b (x\n,y,z) d', 'c', 'd']

        eq_(split_statements(data), result)

    @staticmethod
    def test_multiple_sql():
        """Advanced test with removing comments and empty sql statements
        """
        data = """a; /* This is \n
                  a multiline comment */ b;; \n ; -- Comment \n c; d; """

        result = ['a', 'b', 'c', 'd']

        eq_(split_statements(remove_empty_statements(
            remove_comments(data))), result)

    @staticmethod
    def test_split_escaped_sql():
        """Split SQL statement with strings that have semicolon
        """
        data = "a; xyz='0;0'; c;"
        result = ['a', "xyz='0;0'", 'c']
        eq_(split_statements(data), result)
