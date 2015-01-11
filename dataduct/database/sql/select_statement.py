"""Script containing the SelectStatement object
"""

from .sql_statement import SqlStatement
from ..parsers import parse_select_dependencies
from ..parsers import parse_select_columns


class SelectStatement(SqlStatement):
    """Class representing SelectStatement from a sql_statement
    """
    def __init__(self, sql):
        """Constructor for CreateTableStatement class
        """
        super(SelectStatement, self).__init__(sql)

        self._dependencies = parse_select_dependencies(self.sql())
        self._columns = parse_select_columns(self.sql())

    @property
    def dependencies(self):
        """Table dependencies of the select statement
        """
        return self._dependencies

    @property
    def columns(self):
        """Table columns of the select statement
        """
        return self._columns
