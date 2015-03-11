"""ETL step wrapper for Upsert SQL script
"""
from .create_update_sql import CreateUpdateSqlStep
from ..database import Table
from ..database import SqlScript
from ..database import SelectStatement
from ..database import HistoryTable
from ..utils.helpers import parse_path
from ..utils.helpers import exactly_one


class UpsertStep(CreateUpdateSqlStep):
    """Upsert Step class that helps run a step on the emr cluster
    """

    def __init__(self, destination, sql=None, script=None, source=None,
                 enforce_primary_key=True, delete_existing=False, history=None,
                 analyze_table=True, **kwargs):
        """Constructor for the UpsertStep class

        Args:
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        assert exactly_one(sql, source, script), 'One of sql/source/script'

        # Input formatting
        dest = Table(SqlScript(filename=parse_path(destination)))

        if source is not None:
            source_relation = Table(SqlScript(filename=parse_path(source)))
        else:
            source_relation = SelectStatement(
                SqlScript(sql=sql, filename=parse_path(script)).sql())

        # Create the destination table if doesn't exist
        sql_script = dest.upsert_script(source_relation, enforce_primary_key,
                                        delete_existing)

        if history:
            hist = HistoryTable(SqlScript(
                filename=parse_path(history)))
            sql_script.append(hist.update_history_script(dest))

        super(UpsertStep, self).__init__(
            table_definition=destination, command=sql_script.sql(),
            analyze_table=analyze_table, **kwargs)
