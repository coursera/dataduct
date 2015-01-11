"""Script containing the table class object
"""
from copy import deepcopy

from ..parsers.create_table import parse_create_table
from ..sql.sql_script import SqlScript
from .column import Column


class Table(object):
    """Class representing tables in the database
    """
    def __init__(self, sql):
        """Constructor for Table class
        """

        if isinstance(sql, SqlScript):
            # Take the first statement and ignore the rest
            sql = SqlScript.statements[0]

        parameters = parse_create_table(sql)

        self.sql = sql
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

    def __str__(self):
        """Output for the print statement of the table
        """
        return self.sql

    def copy(self):
        """Create a copy of the Table object
        """
        return deepcopy(self)

    def initialize_name(self):
        """Parse the full name to declare the schema and table name
        """
        split_name = self.full_name.split('.')
        if len(split_name) == 2:
            schema_name = split_name[0]
            table_name = split_name[1]
        else:
            schema_name = None
            table_name = self.full_name

        return schema_name, table_name

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
                               constraint.get('fk_reference_columns')))
        return result

    @property
    def dependencies(self):
        """List of tables which this table references.
        """
        return [table_name for _, table_name, _ in self.foreign_key_references]
