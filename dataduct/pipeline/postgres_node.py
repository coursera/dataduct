"""
Pipeline object class for SqlNode
"""

from ..utils.exceptions import ETLInputError
from .pipeline_object import PipelineObject
from .schedule import Schedule


class PostgresNode(PipelineObject):
    """SQL Data Node class
    """

    def __init__(self, id, schedule, host, database, username, password,
                 select_query, insert_query, table, depends_on=None):
        """Constructor for the SqlNode class

        Args:
            id(str): id of the object
            schedule(Schedule): pipeline schedule
            database(str): database name on the RDS host
            sql(str): sql to be executed
            table(str): table to be read
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        if not depends_on:
            depends_on = list()

        kwargs = {
            'id': id,
            'type': 'SqlDataNode',
            'schedule': schedule,
            'database': database,
            'selectQuery': select_query,
            'insertQuery': insert_query,
            'table': table,
            'dependsOn': depends_on,
        }
        super(PostgresNode, self).__init__(**kwargs)
