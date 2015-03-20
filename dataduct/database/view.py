"""Script containing the view class object
"""
from .parsers import parse_create_view
from .sql import SqlScript
from .select_statement import SelectStatement
from .relation import Relation


class View(Relation):
    """Class representing view in the database
    """
    def __init__(self, sql):
        """Constructor for view class
        """
        super(View, self).__init__()

        if isinstance(sql, SqlScript):
            # Take the first statement and ignore the rest
            sql = sql.statements[0]

        parameters = parse_create_view(sql.sql())

        self.sql_statement = sql
        self.parameters = parameters

        self.full_name = parameters.get('view_name')
        self.replace_flag = parameters.get('replace', False)

        self.select_statement = SelectStatement(parameters.get('select_statement'))

        self.schema_name, self.view_name = self.initialize_name()

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
        return SqlScript('DROP VIEW IF EXISTS %s CASCADE' % self.full_name)

    def check_not_exists_script(self):
        """Sql script to create statement if the table exists or not
        """
        return SqlScript("""
            SELECT NOT EXISTS(
                SELECT 1
                FROM information_schema.views
                WHERE table_schema = '%s'
                AND table_name = '%s'
            )
        """ % (self.schema_name, self.view_name))
