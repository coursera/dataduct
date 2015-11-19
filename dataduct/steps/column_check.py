"""ETL step wrapper for column check step can be executed on Ec2 resource
"""
from ..config import Config
from ..database import SelectStatement
from ..database import SqlScript
from ..database import Table
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from ..utils.helpers import exactly_one
from ..utils.helpers import parse_path
from .qa_transform import QATransformStep

config = Config()
COLUMN_TEMPLATE = "COALESCE(CONCAT({column_name}, ''), '')"


class ColumnCheckStep(QATransformStep):
    """ColumnCheckStep class that checks if the rows of a table has been
    populated with the correct values
    """

    def __init__(self, id, source_sql, source_host,
                 destination_table_definition=None, script=None,
                 destination_sql=None, sql_tail_for_source=None,
                 sample_size=100, tolerance=1.0, script_arguments=None,
                 log_to_s3=False, **kwargs):
        """Constructor for the ColumnCheckStep class

        Args:
            destination_table_definition(file):
                table definition for the destination table
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        if not exactly_one(destination_table_definition, destination_sql):
            raise ETLInputError('One of dest table or dest sql needed')

        if script_arguments is None:
            script_arguments = list()

        if sql_tail_for_source is None:
            sql_tail_for_source = ''

        # Get the EDW column SQL
        dest_sql, primary_key_index = self.convert_destination_to_column_sql(
            destination_table_definition, destination_sql)

        src_sql = self.convert_source_to_column_sql(source_sql,
                                                    primary_key_index,
                                                    sql_tail_for_source)

        script_arguments.extend([
            '--sample_size=%s' % str(sample_size),
            '--tolerance=%s' % str(tolerance),
            '--destination_sql=%s' % dest_sql,
            '--source_sql=%s' % src_sql,
            '--source_host=%s' % source_host
        ])

        if log_to_s3:
            script_arguments.append('--log_to_s3')

        command = None if script else const.COLUMN_CHECK_COMMAND

        super(ColumnCheckStep, self).__init__(
            id=id, script=script, command=command,
            script_arguments=script_arguments, **kwargs)

    @staticmethod
    def convert_destination_to_column_sql(destination_table_definition=None,
                                          destination_sql=None):
        """Convert the destination query into generic structure to compare
        """
        if destination_table_definition is not None:
            with open(parse_path(destination_table_definition)) as f:
                destination_table_string = f.read()

            destination_table = Table(SqlScript(destination_table_string))
            destination_columns = destination_table.columns()
            primary_key_index, primary_keys = zip(*[
                (idx, col.name)
                for idx, col in enumerate(destination_columns)
                if col.primary])

            if len(destination_columns) == len(primary_key_index):
                raise ValueError('Cannot check table without non-pk columns')

            column_string = '||'.join(
                [COLUMN_TEMPLATE.format(column_name=c.name)
                 for c in destination_columns if not c.primary])
            concatenated_column = '( {columns} )'.format(columns=column_string)

            destination_sql = '''SELECT {primary_keys}, {concat_column}
                 FROM {table_name}
                 WHERE ({primary_keys}) IN PRIMARY_KEY_SET
            '''.format(primary_keys=','.join(primary_keys),
                       concat_column=concatenated_column,
                       table_name=destination_table.full_name)

        elif destination_sql is not None:
            select_stmnt = SelectStatement(destination_sql)
            primary_key_index = range(len(select_stmnt.columns()))[:-1]

        return SqlScript(destination_sql).sql(), primary_key_index

    @staticmethod
    def convert_source_to_column_sql(source_sql, primary_key_index,
                                     sql_tail_for_source):
        """Convert the source query into generic structure to compare
        """
        origin_sql = SelectStatement(SqlScript(source_sql).statements[0].sql())

        # Remove column name references to tables as t.session_id should be
        # session_id as we wrap the whole query.
        column_names = [x.name.split('.')[-1] for x in origin_sql.columns()]

        non_primary_key_index = [idx for idx in range(len(column_names))
                                 if idx not in primary_key_index]

        primary_key_str = ','.join(
            [column_names[idx] for idx in primary_key_index])

        if len(column_names) == len(primary_key_index):
            raise ValueError('Cannot check column on table with no pk columns')

        column_string = ','.join(
            [COLUMN_TEMPLATE.format(column_name=column_names[idx])
             for idx in non_primary_key_index])
        concatenated_column = ('CONCAT(%s)' % column_string)

        template = '''SELECT {primary_keys}, {concat_column} AS merged_string
                      FROM ({origin_sql}) AS origin {sql_tail}'''

        query = template.format(primary_keys=primary_key_str,
                                concat_column=concatenated_column,
                                origin_sql=origin_sql.sql(),
                                sql_tail=sql_tail_for_source)

        return SqlScript(query).sql()
