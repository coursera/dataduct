import os
from ..utils.hook import hook

import logging
logger = logging.getLogger(__name__)

@hook('connect_to_redshift')
def open_psql_shell(redshift_creds, **kwargs):
    command = [
        "psql",
        "-h", redshift_creds["HOST"],
        "-p", str(redshift_creds["PORT"]),
        "-U", redshift_creds["USERNAME"],
        "-d", redshift_creds["DATABASE_NAME"],
        "-vPROMPT1=%[%033[0m%]" + redshift_creds["CLUSTER_ID"] + "%R%[%033[0m%]%# ",
        "-vPROMPT2=%[%033[0m%]" + redshift_creds["CLUSTER_ID"] + "%R%[%033[0m%]%# ",
        ]
    env = dict(os.environ)
    env['PGPASSWORD'] = redshift_creds["PASSWORD"]
    logger.info("Running command: {}".format(' '.join(command)))
    os.execvpe(command[0], command, env=env)


@hook('connect_to_mysql')
def open_mysql_shell(sql_creds, **kwargs):
    command = [
        "mysql",
        "-h", sql_creds["HOST"],
        "-u", sql_creds["USERNAME"],
        "--default-character-set=utf8"
    ]
    if sql_creds.get("DATABASE"):
        command.extend(["-D", sql_creds["DATABASE"]])

    env = dict(os.environ)
    env['MYSQL_PWD'] = sql_creds["PASSWORD"]
    logger.info("Running command: {}".format(' '.join(command)))
    os.execvpe(command[0], command, env=env)
