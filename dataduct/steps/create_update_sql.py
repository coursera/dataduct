"""
ETL step wrapper for sql command for inserting into tables
"""
from .sql_command import SqlCommandStep
from ..database import SqlScript
from ..database import Table
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from ..utils.exceptions import ETLInputError


class CreateUpdateSqlStep(SqlCommandStep):
    """Create and Insert step that creates a table and then uses the query to
    update the table data with any sql query provided
    """

    def __init__(self,
                 table_definition,
                 script=None,
                 command=None,
                 analyze_table=True,
                 wrap_transaction=True,
                 **kwargs):
        """Constructor for the CreateUpdateStep class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        if not exactly_one(command, script):
            raise ETLInputError('Both command or script found')

        # Create S3File with script / command provided
        if script:
            update_script = SqlScript(filename=parse_path(script))
        else:
            update_script = SqlScript(command)

        dest = Table(SqlScript(filename=parse_path(table_definition)))

        sql_script = dest.exists_clone_script()
        sql_script.append(dest.grant_script())
        sql_script.append(update_script)

        if wrap_transaction:
            sql_script = sql_script.wrap_transaction()

        # Analyze cannot be done inside a transaction
        if analyze_table:
            sql_script.append(dest.analyze_script())

        super(CreateUpdateSqlStep, self).__init__(
            sql_script=sql_script, wrap_transaction=False, **kwargs)
