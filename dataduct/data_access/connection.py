"""
Connections to various databases such as RDS and Redshift
"""
import psycopg2
import MySQLdb

from ..config import Config
from ..utils.helpers import retry

config = Config()

@retry(2, 60)
def redshift_connection(**kwargs):
    """Fetch a psql connection object to redshift
    """
    connection = psycopg2.connect(
        host=config.redshift['HOST'],
        user=config.redshift['USERNAME'],
        password=config.redshift['PASSWORD'],
        port=config.redshift['PORT'],
        database=config.redshift['DATABASE_NAME'],
        connect_timeout=10,
        **kwargs
    )

    return connection

@retry(2, 60)
def rds_connection(host_name, cursorclass=MySQLdb.cursors.SSCursor,
                   **kwargs):
    """Fetch a psql connection object to redshift
    """
    sql_creds = config.mysql[host_name]

    connection = MySQLdb.connect(
        host=sql_creds['HOST'],
        user=sql_creds['USERNAME'],
        passwd=sql_creds['PASSWORD'],
        db=host_name,
        charset='utf8',      # Necessary for foreign chars
        cursorclass=cursorclass,
        **kwargs
    )

    return connection
