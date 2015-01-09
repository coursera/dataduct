"""Script that contains the sql statement class
"""

from .utils import sanatize_sql


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
        self._raw_statement = self._sanatize_sql()

    def __str__(self):
        """Print a SqlStatement object
        """
        return self.sql()

    def sql(self):
        """Returns the raw_sql for the SqlStatement
        """
        return self._raw_statement

    def _sanatize_sql(self):
        """Clean the SQL, remove comments and empty statements
        """
        if self._raw_sql is None:
            return ''

        raw_statements = sanatize_sql(self._raw_sql, self.transactional)

        if len(raw_statements) > 1:
            raise ValueError('SQL Statement can not contain more than 1 query')
        elif len(raw_statements) == 1:
            return raw_statements[0]
        else:
            return ''
