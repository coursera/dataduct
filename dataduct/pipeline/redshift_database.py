"""
Pipeline object class for redshift database
"""

from ..config import Config
from .pipeline_object import PipelineObject

config = Config()
REDSHIFT_DATABASE_NAME = config.redshift['REDSHIFT_DATABASE_NAME']
REDSHIFT_CLUSTER_ID = config.redshift['REDSHIFT_CLUSTER_ID']
REDSHIFT_USERNAME = config.redshift['REDSHIFT_USERNAME']
REDSHIFT_PASSWORD = config.redshift['REDSHIFT_PASSWORD']


class RedshiftDatabase(PipelineObject):
    """Redshift resource class
    """

    def __init__(self,
                 id,
                 database_name=REDSHIFT_DATABASE_NAME,
                 cluster_id=REDSHIFT_CLUSTER_ID,
                 username=REDSHIFT_USERNAME,
                 password=REDSHIFT_PASSWORD):
        """Constructor for the RedshiftDatabase class

        Args:
            id(str): id of the object
            database_name(str): host name of the database
            cluster_id(str): identifier for the redshift database across aws
            username(str): username for the database
            password(str): password for the database
        """

        kwargs = {
            'id': id,
            'type': 'RedshiftDatabase',
            'databaseName': database_name,
            'clusterId': cluster_id,
            'username': username,
            '*password': password,
        }
        super(RedshiftDatabase, self).__init__(**kwargs)
