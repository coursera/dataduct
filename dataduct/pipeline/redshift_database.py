"""
Pipeline object class for redshift database
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError

config = Config()

if not hasattr(config, 'redshift'):
    raise ETLConfigError('Redshift credentials missing from config')

CLUSTER_ID = None
CONNECTION_STRING = None
DATABASE_NAME = config.redshift['DATABASE_NAME']
USERNAME = config.redshift['USERNAME']
PASSWORD = config.redshift['PASSWORD']


#If both are there, we will pick the Connection String. Cluster ID is required for the Redshift shell to work
if 'CLUSTER_ID' in config.redshift and 'CONNECTION_STRING' in config.redshift:
    CONNECTION_STRING = config.redshift['CONNECTION_STRING'] 
elif 'CLUSTER_ID' in config.redshift:
    CLUSTER_ID = config.redshift['CLUSTER_ID']
elif 'CONNECTION_STRING' in config.redshift:
    raise ETLConfigError('Redshift credentials - CLUSTER_ID is also required while connecting using CONNECTION_STRING.')


class RedshiftDatabase(PipelineObject):
    """Redshift resource class
    """

    def __init__(self,
                 id,
                 database_name=DATABASE_NAME,
                 cluster_id=CLUSTER_ID,
                 connection_string=CONNECTION_STRING,
                 username=USERNAME,
                 password=PASSWORD):
        """Constructor for the RedshiftDatabase class

        Args:
            id(str): id of the object
            database_name(str): host name of the database
            cluster_id(str): identifier for the redshift database across aws
            connection_string(str): JDBC connection string of the Redshift.
            username(str): username for the database
            password(str): password for the database
        """

        kwargs = {
            'id': id,
            'type': 'RedshiftDatabase',
            'databaseName': database_name,
            'username': username,
            '*password': password
        }

        if CLUSTER_ID:
            kwargs['clusterId'] = CLUSTER_ID
        else:
            kwargs['connectionString'] = CONNECTION_STRING

        super(RedshiftDatabase, self).__init__(**kwargs)
