"""ETL step wrapper for delta loading a table based on a date column
"""
from ..database import SqlScript
from ..database import Table
from ..utils.helpers import parse_path
from .upsert import UpsertStep


class DeltaLoadStep(UpsertStep):
    """DeltaLoadStep Step class that creates the table if needed and loads data
    """

    def __init__(self, destination, date_column, window=0, **kwargs):
        """Constructor for the DeltaLoadStep class

        Args:
            date_column(string): name of column (of type date) to use as the
                delta value (i.e., only load the last X days)
            window(int): number of days before last loaded day to update
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        dest = Table(SqlScript(filename=parse_path(destination)))
        delta_clause = """
            WHERE {date_column} >=
                COALESCE(
                    (SELECT MAX({date_column}) FROM {destination}),
                    '1800-01-01'::DATE
                ) - {window}
        """.format(date_column=date_column,
                   destination=dest.full_name,
                   window=window)
        super(DeltaLoadStep, self).__init__(destination=destination,
                                            filter_clause=delta_clause,
                                            **kwargs)
