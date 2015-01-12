"""Script containing the table class object
"""
from .parsers import parse_create_table
from .sql import SqlScript
from .column import Column
from .relation import Relation


class Table(Relation):
    """Class representing tables in the database
    """
    def __init__(self, sql):
        """Constructor for Table class
        """
        super(Table, self).__init__()

        if isinstance(sql, SqlScript):
            # Take the first statement and ignore the rest
            sql = SqlScript.statements[0]

        parameters = parse_create_table(sql)

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

    @property
    def columns(self):
        """Columns for the table
        """
        return self._columns.values()

    @property
    def primary_keys(self):
        """Primary keys of the table
        """
        return [c for c in self.columns if c.primary]

    def forign_key_references(self):
        """Get a list of all foreign key references from the table
        """
        result = list()
        for column in self.columns:
            if column.fk_table is not None:
                result.append((
                    [column.name], column.fk_table, column.fk_reference))

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
        return [table_name for _, table_name, _ in self.foreign_key_references]

    def temporary_clone_script(self):
        """Sql script to create a temporary clone table

        Note:
            The temporary table only copies the schema and not any data
        """

        # We don't need to use schema for temp tables
        table_name = self.table_name + '_temp'

        # Create a list of column definitions
        columns = ', '.join(
            ['%s %s' %(c.column_name, c.column_type) for c in self.columns])

        # We don't need any constraints to be specified on the temp table
        sql = ['CREATE TEMPORARY TABLE %s ( %s )' % (table_name, columns)]

        return SqlScript(sql)

    def drop_script(self):
        """Sql script to drop the table
        """
        return SqlScript('DROP TABLE IF EXISTS %s CASCADE' % self.full_name)

    def analyze_script(self):
        """Sql script to analyze the table
        """
        return SqlScript('ANALYZE %s' % self.full_name)

    def foreign_key_reference_script(self, source_columns, reference_name,
                                     reference_columns):
        """Sql Script to create a FK reference from table x to y
        """
        sql = """
            ALTER TABLE {source_name}
            ADD FOREIGN KEY ({source_columns})
            REFERENCES {reference_name} ({reference_columns})
        """.format(source_name=self.full_name,
                   source_columns=', '.join(source_columns),
                   reference_name=reference_name,
                   reference_columns=', '.join(reference_columns))

        return SqlScript(sql)
