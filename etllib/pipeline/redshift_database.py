"""
Pipeline object class for redshift database
"""
from ..constants import REDSHIFT_DATABASE_NAME
from ..constants import REDSHIFT_CLUSTER_ID
from ..constants import REDSHIFT_USERNAME
from ..constants import REDSHIFT_PASSWORD

from .pipeline_object import PipelineObject


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
