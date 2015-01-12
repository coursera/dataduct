"""Script containing the view class object
"""
from copy import deepcopy

from .parsers import parse_create_view
from .sql import SqlScript
from .sql import SelectStatement


class View(object):
    """Class representing view in the database
    """
    def __init__(self, sql):
        """Constructor for view class
        """

        if isinstance(sql, SqlScript):
            # Take the first statement and ignore the rest
            sql = SqlScript.statements[0]

        parameters = parse_create_view(sql)

        self.sql_statement = sql
        self.parameters = parameters

        self.full_name = parameters.get('view_name')
        self.replace_flag = parameters.get('replace', False)

        self.select_statement = SelectStatement(parameters.get('select_statement'))

        self.schema_name, self.view_name = self.initialize_name()

    def __str__(self):
        """Output for the print statement of the view
        """
        return self.sql_statement

    def copy(self):
        """Create a copy of the view object
        """
        return deepcopy(self)

    def initialize_name(self):
        """Parse the full name to declare the schema and view name
        """
        split_name = self.full_name.split('.')
        if len(split_name) == 2:
            schema_name = split_name[0]
            view_name = split_name[1]
        else:
            schema_name = None
            view_name = self.view_name

        return schema_name, view_name

    @property
    def dependencies(self):
        """List of relations which this view references.
        """
        return self.select_statement.dependencies

    @property
    def columns(self):
        """List of columns in the view's select statement
        """
        return self.select_statement.columns

    def drop_script(self):
        """Sql script to drop the view
        """
        return SqlScript('DROP VIEW %s CASCADE' % self.full_name)
