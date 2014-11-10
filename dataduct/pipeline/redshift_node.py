"""
Pipeline object class for RedshiftNode
"""

from .pipeline_object import PipelineObject
from .schedule import Schedule
from ..utils.exceptions import ETLInputError


class RedshiftNode(PipelineObject):
    """Redshift Data Node class
    """

    def __init__(self,
                 id,
                 schedule,
                 redshift_database,
                 schema_name,
                 table_name):
        """Constructor for the RedshiftNode class

        Args:
            id(str): id of the object
            schedule(Schedule): pipeline schedule
            redshift_database(RedshiftDatabase): database for the node
            schema_name(str): schema for node to extract or load data
            table_name(str): table for node to extract or load data
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        super(RedshiftNode, self).__init__(
            id=id,
            type='RedshiftDataNode',
            schedule=schedule,
            database=redshift_database,
            schemaName=schema_name,
            tableName=table_name,
        )

    @property
    def schema(self):
        """Get the schema name for the redshift node

        Returns:
            result(str): schema name for this redshift node
        """
        return self['schemaName']

    @property
    def table(self):
        """Get the table name for the redshift node

        Returns:
            result(str): table name for this redshift node
        """
        return self['tableName']
