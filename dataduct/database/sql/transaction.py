"""SQL Statements used in transactions
"""

from .sql_statement import SqlStatement


class BeginStatement(SqlStatement):
    """Class representing begin sql statement
    """
    def __init__(self):
        """Constructor for begin class
        """
        super(BeginStatement, self).__init__('BEGIN', True)


class CommitStatement(SqlStatement):
    """Class representing Commit sql statement
    """
    def __init__(self):
        """Constructor for Commit class
        """
        super(CommitStatement, self).__init__('COMMIT', True)
