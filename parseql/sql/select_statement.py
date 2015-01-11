"""Script containing the SelectStatement object
"""

from .sql_statement import SqlStatement


class SelectStatement(SqlStatement):
    """Class representing SelectStatement from a sql_statement
    """
    def __init__(self, sql):
        """Constructor for CreateTableStatement class
        """
        super(SelectStatement, self).__init__(sql)
