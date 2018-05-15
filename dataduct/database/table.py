"""Script containing the table class object
"""
from ..utils.helpers import stringify_credentials
from .column import Column
from .parsers import create_exists_clone
from .parsers import parse_create_table
from .relation import Relation
from .select_statement import SelectStatement
from .sql import SqlScript

import logging
logger = logging.getLogger(__name__)


def comma_seperated(elements):
    """Create a comma separated string from the iterator
    """
    return ','.join(elements)


class Table(Relation):
    """Class representing tables in the database
    """
    def __init__(self, sql):
        """Constructor for Table class

        Args:
            sql: A SqlScript or a string containing the SQL definition of a
                 table. If sql is a string, it will be converted into a
                 SqlScript.
        """
        super(Table, self).__init__()

        if isinstance(sql, str):
            sql = SqlScript(sql)

        if isinstance(sql, SqlScript):
            # Take the first statement and ignore the rest
            sql = sql.statements[0]

        parameters = parse_create_table(sql.sql())

        self.sql_statement = sql
        self.parameters = parameters

        self.full_name = parameters.get('full_name')
        self.temporary = parameters.get('temporary')
        self.exists_check = parameters.get('exists_check', False)

        self.sort_keys = parameters.get('sortkey', list())
        self.dist_keys = parameters.get('distkey', list())
        self.diststyle = parameters.get('diststyle', 'EVEN')

        self._constraints = parameters.get('constraints', list())

        self._columns = dict()
        for column_params in parameters.get('columns', list()):
            column_name = column_params['column_name']
            self._columns[column_name] = Column(**column_params)

        self.schema_name, self.table_name = self.initialize_name()
        self.update_attributes_from_columns()
        self.update_columns_with_constrains()

    def update_attributes_from_columns(self):
        """ Update attributes sortkey and distkey based on columns
        """
        distkeys = self.dist_keys
        sortkeys = self.sort_keys
        for column in self._columns.values():
            # Update the table attributes based on columns
            if column.is_distkey:
                distkeys.append(column.name)
            if column.is_sortkey:
                sortkeys.append(column.name)

        self.dist_keys = list(set(distkeys))
        self.sort_keys = list(set(sortkeys))

    def update_columns_with_constrains(self):
        """ Update columns with primary and foreign key constraints
        """
        for constraint in self._constraints:
            for col_name in constraint.get('pk_columns', list()):
                self._columns[col_name].primary = True

    def columns(self):
        """Unsorted list of columns in the table
        """
        return sorted(self._columns.values(), key=lambda x: x.position)

    def column(self, column_name):
        """Get the column with the given name
        """
        return self._columns.get(column_name, None)

    @property
    def primary_keys(self):
        """Primary keys of the table
        """
        return [c for c in self.columns() if c.primary]

    @property
    def primary_key_names(self):
        """Primary keys of the table
        """
        return [c.name for c in self.columns() if c.primary]

    def foreign_key_references(self):
        """Get a list of all foreign key references from the table
        """
        result = list()
        for column in self.columns():
            if column.fk_table is not None:
                result.append((
                    [column.name], column.fk_table, [column.fk_reference]))

        for constraint in self._constraints:
            if 'fk_table' in constraint:
                result.append((constraint.get('fk_columns'),
                               constraint.get('fk_table'),
                               constraint.get('fk_reference')))
        return result

    @property
    def dependencies(self):
        """List of tables which this table references.
        """
        return [table_name for _, table_name, _
                in self.foreign_key_references()]

    def temporary_clone_script(self):
        """Sql script to create a temporary clone table

        Note:
            The temporary table only copies the schema and not any data
        """

        # We don't need to use schema for temp tables
        table_name = self.table_name + '_temp'

        # Create a list of column definitions
        columns = comma_seperated(
            ['%s %s' % (c.column_name, c.column_type) for c in self.columns()])

        if self.primary_keys:
            sql = """CREATE TEMPORARY TABLE {table_name} (
                        {columns},
                        PRIMARY KEY( {primary_keys} )
                  )""".format(
                  table_name=table_name, columns=columns,
                  primary_keys=comma_seperated(self.primary_key_names))
        else:
            sql = """CREATE TEMPORARY TABLE {table_name} (
                        {columns}
                  )""".format(
                  table_name=table_name, columns=columns)

        return SqlScript(sql)

    def exists_clone_script(self):
        """Sql script to create a exists clone table
        """
        return SqlScript(create_exists_clone(self.sql_statement.sql()))

    def drop_script(self):
        """Sql script to drop the table
        """
        return SqlScript('DROP TABLE IF EXISTS %s CASCADE' % self.full_name)

    def analyze_script(self):
        """Sql script to analyze the table
        """
        return SqlScript('ANALYZE %s' % self.full_name)

    def rename_script(self, new_name):
        """Sql script to rename the table
        """
        return SqlScript(
            'ALTER TABLE %s RENAME TO %s' % (self.full_name, new_name))

    def delete_script(self, where_condition=''):
        """Sql script to delete from table based on where condition
        """
        return SqlScript(
            'DELETE FROM %s %s' % (self.full_name, where_condition))

    def foreign_key_reference_script(self, source_columns, reference_name,
                                     reference_columns):
        """Sql Script to create a FK reference from table x to y
        """
        sql = """
            ALTER TABLE {source_name}
            ADD FOREIGN KEY ({source_columns})
            REFERENCES {reference_name} ({reference_columns})
        """.format(source_name=self.full_name,
                   source_columns=comma_seperated(source_columns),
                   reference_name=reference_name,
                   reference_columns=comma_seperated(reference_columns))

        return SqlScript(sql)

    def select_duplicates_script(self):
        """Sql Script to select duplicate primary keys from the table
        """
        pk_columns = comma_seperated(self.primary_key_names)
        sql = """
            SELECT {pk_columns}
                ,COUNT(1) duplicate_count
            FROM {table_name}
            GROUP BY {pk_columns}
            HAVING COUNT(1) > 1
        """.format(table_name=self.full_name,
                   pk_columns=pk_columns)

        return SqlScript(sql)

    def _source_sql(self, source_relation, filter_clause=None):
        """Get the source sql based on the type of the source specified
        """
        if not (isinstance(source_relation, Relation) or
                isinstance(source_relation, SelectStatement)):
            raise ValueError('Source Relation must be a relation or select')

        if len(self.columns()) < len(source_relation.columns()):
            raise ValueError('Source has more columns than destination')

        if isinstance(source_relation, SelectStatement):
            source_sql = '(' + source_relation.sql() + ')'
        else:
            source_sql = source_relation.full_name

        if filter_clause is not None:
            source_sql = source_sql + ' ' + filter_clause

        return source_sql

    def insert_script(self, source_relation, filter_clause=None):
        """Sql Script to insert into the table while avoiding PK violations
        """
        sql = 'INSERT INTO %s (SELECT * FROM %s)' % (
            self.full_name, self._source_sql(source_relation, filter_clause))
        return SqlScript(sql)

    def delete_matching_rows_script(self, source_relation):
        """Sql Script to delete matching rows between table and source
        """
        if len(self.primary_keys) == 0:
            raise RuntimeError(
                'Cannot delete matching rows from table with no primary keys')

        source_col_names, pk_names = [], []
        for column in self.columns():
            if column.primary:
                pk_names.append(column.name)
                source_col_names.append(column.name)

        where_condition = 'WHERE (%s) IN (SELECT DISTINCT %s FROM %s)' % (
            comma_seperated(pk_names), comma_seperated(source_col_names),
            self._source_sql(source_relation))

        return self.delete_script(where_condition)

    def de_duplication_script(self):
        """De-duplicate the table to enforce primary keys
        """
        if len(self.primary_keys) == 0:
            logger.error(
                'Cannot de-duplicate table with no primary keys')
            return SqlScript()

        script = self.temporary_clone_script()
        column_names = [c.name for c in self.columns()]

        # Create a temporary clone from the script
        temp_table = self.__class__(script)
        script.append(temp_table.insert_script(self))
        script.append(self.delete_script())

        # Pick a random value on multiple primary keys
        sql = """
            INSERT INTO {table_name} (
                SELECT {column_names}
                FROM (
                    SELECT *,
                    COUNT(1) OVER (
                        PARTITION BY {pk_names}
                        ORDER BY 1 ROWS UNBOUNDED PRECEDING) rnk
                    FROM {temp_table})
                WHERE rnk = 1)
        """.format(table_name=self.full_name,
                   column_names=comma_seperated(column_names),
                   pk_names=comma_seperated(self.primary_key_names),
                   temp_table=temp_table.full_name)

        script.append(SqlScript(sql))
        return script

    def upsert_script(self, source_relation, enforce_primary_key=True,
                      delete_existing=False, filter_clause=None):
        """Sql script to upsert into a table

        The script first copies all the source data into a temporary table.
        Then if the enforce_primary_key flag is set we de-duplicate the temp
        table. After which if the delete existing flag is set we delete all
        the data from the destination table otherwise only the rows that match
        the temporary table. After which we copy the temporary table into the
        destination table.
        """
        script = self.temporary_clone_script()

        # Create a temporary clone from the script
        temp_table = self.__class__(script)
        script.append(temp_table.insert_script(source_relation, filter_clause))
        if enforce_primary_key:
            script.append(temp_table.de_duplication_script())

        if delete_existing:
            script.append(self.delete_script())
        else:
            script.append(self.delete_matching_rows_script(temp_table))

        script.append(self.insert_script(temp_table))
        script.append(temp_table.drop_script())
        return script

    def check_not_exists_script(self):
        """Sql script to create statement if the table exists or not
        """
        return SqlScript("""
            SELECT NOT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = '%s'
                AND table_name = '%s'
            )
        """ % (self.schema_name, self.table_name))

    def unload_script(self, s3_path, access_key, secret_key, token=None):
        """Sql script to unload table to S3
        """
        script = (
            "UNLOAD ('{select_script}') TO '{s3_path}' CREDENTIALS '{creds}' "
            "DELIMITER '\t' ESCAPE NULL AS 'NULL'"
        ).format(
            select_script=self.select_script(),
            s3_path=s3_path,
            creds=stringify_credentials(access_key, secret_key, token)
        )
        return SqlScript(script)

    def load_script(self, s3_path, access_key, secret_key, token=None):
        """Sql script to load table from S3
        """
        script = (
            "COPY {name} FROM '{s3_path}' CREDENTIALS '{creds}' "
            "DELIMITER '\t' ESCAPE NULL AS 'NULL'"
        ).format(
            name=self.full_name,
            s3_path=s3_path,
            creds=stringify_credentials(access_key, secret_key, token)
        )
        return SqlScript(script)
