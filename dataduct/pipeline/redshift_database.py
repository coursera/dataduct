"""
Pipeline object class for redshift database
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError

config = Config()

if not hasattr(config, 'redshift'):
    raise ETLConfigError('Redshift credentials missing from config')

DATABASE_NAME = config.redshift['DATABASE_NAME']
CLUSTER_ID = config.redshift['CLUSTER_ID']
USERNAME = config.redshift['USERNAME']
PASSWORD = config.redshift['PASSWORD']


class RedshiftDatabase(PipelineObject):
    """Redshift resource class
    """

    def __init__(self,
                 id,
                 database_name=DATABASE_NAME,
                 cluster_id=CLUSTER_ID,
                 username=USERNAME,
                 password=PASSWORD):
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
