"""Script that contains the sql statement class
"""
from copy import deepcopy
from .utils import sanitize_sql
from ..parsers import parse_create_table
from ..parsers import parse_create_view


class SqlStatement(object):
    """Class representing a single SQL statement
    """
    def __init__(self, sql=None, transactional=False):
        """Constructor for the SqlStatement class
        """
        if sql is None:
            sql = ''
        self._raw_sql = sql
        self.transactional = transactional
        self._raw_statement = self._sanitize_sql()

    def __str__(self):
        """Print a SqlStatement object
        """
        return self.sql()

    def copy(self):
        """Create a copy of the relation object
        """
        return deepcopy(self)

    def sql(self):
        """Returns the raw_sql for the SqlStatement
        """
        return self._raw_statement

    def _sanitize_sql(self):
        """Clean the SQL, remove comments and empty statements
        """
        if self._raw_sql is None:
            return ''

        raw_statements = sanitize_sql(self._raw_sql, self.transactional)

        if len(raw_statements) > 1:
            raise ValueError('SQL Statement can not contain more than 1 query')
        elif len(raw_statements) == 1:
            return raw_statements[0]
        else:
            return ''

    def _validate_parser(self, func):
        """Check if a parser satisfies the sql statement
        """
        try:
            func(self.sql())
        except Exception:
            return False
        return True

    def creates_table(self):
        """SQL statement creates a table.
        """
        return self._validate_parser(parse_create_table)

    def creates_view(self):
        """SQL statement creates a view.
        """
        return self._validate_parser(parse_create_view)
