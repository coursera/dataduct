"""Script that contains the sql script class
"""
from copy import deepcopy

from .sql_statement import SqlStatement
from .transaction import BeginStatement
from .transaction import CommitStatement
from .utils import atmost_one
from .utils import sanatize_sql


class SqlScript(object):
    """Class representing a single SQL Script
    """
    def __init__(self, sql=None, statements=None, filename=None):
        """Constructor for the SqlScript class
        """
        assert atmost_one(sql, statements, filename), 'Multiple intializer'

        if sql is None:
            sql = ''

        if filename:
            with open(filename, 'r') as f:
                sql = f.read()

        self._raw_sql = sql
        self._raw_statements = self._sanatize_sql()
        self._statements = self.initialize_statements()

        # Add the statements that the script was initialized from
        self.append(statements)

    def __str__(self):
        """Print a SqlScript object
        """
        return self.sql()

    def __iter__(self):
        """Iterator for iterating over all the sql statements
        """
        return iter(self._statements)

    def __len__(self):
        """Length of the sqlscript
        """
        return len(self._statements)

    @property
    def statements(self):
        """Returns the SQLStatements of the script
        """
        return self._statements

    def sql(self):
        """Returns the sql for the SqlScript
        """
        return ';\n'.join([x.sql() for x in self._statements])

    def _sanatize_sql(self):
        """Clean the SQL, remove comments and empty statements
        """
        return sanatize_sql(self._raw_sql)

    def _initialize_statements(self):
        """Initialize SQL Statements based on the inputscipt
        """
        return [SqlStatement(x) for x in self._raw_statements]

    def copy(self):
        """Create a copy of the SQL Script object
        """
        return deepcopy(self)

    def append(self, elements):
        """Append the elements to the SQL script
        """
        if isinstance(elements, SqlStatement):
            self.add_statement(elements)
            return self.copy()

        if isinstance(elements, str):
            elements = self.__class__(elements)

        for element in elements:
            self.add_statement(element)

        return self.copy()

    def add_statement(self, statement):
        """Add a single SqlStatement to the SQL Script
        """
        if not isinstance(statement, SqlStatement):
            raise ValueError('Input must be of the type SqlStatement')

        self._statements.append(statement)
        self._raw_statements.append(statement.sql())

    def wrap_transaction(self):
        """Wrap the script in transaction
        """
        new_script = self.__class__()
        new_script.append([BeginStatement(), self, CommitStatement()])
        return new_script
