"""Script containing the SelectStatement object
"""

from .sql import SqlStatement
from .column import Column
from .parsers import parse_select_dependencies
from .parsers import parse_select_columns
from .parsers import parse_column_name


class SelectStatement(SqlStatement):
    """Class representing SelectStatement from a sql_statement
    """
    def __init__(self, sql):
        """Constructor for CreateTableStatement class
        """
        super(SelectStatement, self).__init__(sql)

        self._dependencies = parse_select_dependencies(self.sql())
        self._raw_columns = parse_select_columns(self.sql())
        self._columns = [
            Column(parse_column_name(c), None) for c in self._raw_columns]

    @property
    def dependencies(self):
        """Table dependencies of the select statement
        """
        return self._dependencies

    def columns(self):
        """Table columns of the select statement
        """
        return self._columns
