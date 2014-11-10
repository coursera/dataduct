"""
Pipeline object class for MysqlNode
"""

from .pipeline_object import PipelineObject
from .schedule import Schedule
from ..utils.exceptions import ETLInputError


class MysqlNode(PipelineObject):
    """MySQL Data Node class
    """

    def __init__(self, id, schedule, host, database, username, password, sql,
                 table, depends_on=None):
        """Constructor for the MysqlNode class

        Args:
            id(str): id of the object
            schedule(Schedule): pipeline schedule
            host(str): hostname for the mysql database
            database(str): database name on the RDS host
            user(str): username for the database
            password(str): password for the database
            sql(str): sql to be executed
            table(str): table to be read
        """

        # Validate inputs
        if not isinstance(schedule, Schedule):
            raise ETLInputError(
                'Input schedule must be of the type Schedule')

        if not depends_on:
            depends_on = list()

        connection_string = "jdbc:mysql://" + host + ":3306/" + database

        kwargs = {
            'id': id,
            'type': 'SqlDataNode',
            'schedule': schedule,
            'connectionString': connection_string,
            'username': username,
            '*password': password,
            'selectQuery': sql,
            'table': table,
            'dependsOn': depends_on,
        }
        super(MysqlNode, self).__init__(**kwargs)

    @property
    def database(self):
        """Get the database name for the MySQL node

        Returns:
            result(str): database name for this MySQL node
        """
        return self['connectionString'].split("/").pop()

    @property
    def table(self):
        """Get the table name for the MySQL node

        Returns:
            result(str): table name for this MySQL node
        """
        return self['tableName']
