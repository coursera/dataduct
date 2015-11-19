"""ETL step wrapper for count check step can be executed on the Ec2 resource
"""
from ..config import Config
from ..database import SqlScript
from ..database import SqlStatement
from ..database import Table
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from .qa_transform import QATransformStep

config = Config()


class CountCheckStep(QATransformStep):
    """CountCheckStep class that compares the number of rows in the source
       select script with the number of rows in the destination table
    """

    def __init__(self, id, source_host, source_sql=None,
                 source_table_name=None, destination_table_name=None,
                 destination_table_definition=None, destination_sql=None,
                 tolerance=1.0, script_arguments=None, log_to_s3=False,
                 script=None, source_count_sql=None, **kwargs):
        """Constructor for the CountCheckStep class

        Args:
            source_sql(str): SQL select script from the source table
            destination_table_name(str): table name for the destination table
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if not exactly_one(destination_table_name, destination_sql,
                           destination_table_definition):
            raise ETLInputError(
                'One of dest table name/schema or dest sql needed')

        if not exactly_one(source_sql, source_table_name, source_count_sql):
            raise ETLInputError('One of source table name or source sql ' +
                                'or source count needed')

        if script_arguments is None:
            script_arguments = list()

        if destination_table_definition is not None:
            with open(parse_path(destination_table_definition)) as f:
                destination_table_string = f.read()
            destination_table = Table(SqlScript(destination_table_string))
            destination_table_name = destination_table.full_name

        # Get the EDW column SQL
        dest_sql = self.convert_destination_to_count_sql(
            destination_table_name, destination_sql)

        src_sql = self.convert_source_to_count_sql(
            source_table_name, source_sql, source_count_sql)

        script_arguments.extend([
            '--tolerance=%s' % str(tolerance),
            '--destination_sql=%s' % dest_sql,
            '--source_sql=%s' % src_sql,
            '--source_host=%s' % source_host
        ])

        if log_to_s3:
            script_arguments.append('--log_to_s3')

        command = None if script else const.COUNT_CHECK_COMMAND

        super(CountCheckStep, self).__init__(
            id=id, command=command, script=script,
            script_arguments=script_arguments, **kwargs)

    @staticmethod
    def convert_destination_to_count_sql(destination_table=None,
                                         destination_sql=None):
        """Convert the destination query into generic structure to compare
        """
        if destination_table is not None:
            destination_sql = "SELECT COUNT(1) FROM %s" % destination_table
        else:
            dest_sql = SqlStatement(destination_sql)
            destination_sql = "SELECT COUNT(1) FROM (%s)a" % dest_sql.sql()

        return SqlScript(destination_sql).sql()

    @staticmethod
    def convert_source_to_count_sql(source_table_name=None,
                                    source_sql=None,
                                    source_count_sql=None):
        """Convert the source query into generic structure to compare
        """
        if source_table_name is not None:
            source_sql = "SELECT COUNT(1) FROM %s" % source_table_name
        elif source_count_sql is not None:
            source_sql = source_count_sql
        else:
            origin_sql = SqlStatement(source_sql)
            source_sql = "SELECT COUNT(1) FROM (%s)a" % origin_sql.sql()

        return SqlScript(source_sql).sql()
