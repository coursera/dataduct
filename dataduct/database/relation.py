"""Script containing the relation class object
"""
from copy import deepcopy
from .sql import SqlScript
from ..config import Config
from ..utils.helpers import atleast_one


class Relation(object):
    """Class representing a relation in the database
    """

    def __str__(self):
        """Output for the print statement of the relation
        """
        return self.sql_statement.sql()

    def sql(self):
        """SqlStatement for the table object
        """
        return self.sql_statement

    def copy(self):
        """Create a copy of the relation object
        """
        return deepcopy(self)

    def initialize_name(self):
        """Parse the full name to declare the schema and relation name
        """
        split_name = self.full_name.split('.')
        if len(split_name) == 2:
            schema_name = split_name[0]
            relation_name = split_name[1]
        else:
            schema_name = None
            relation_name = self.full_name

        return schema_name, relation_name

    def _grant_sql_builder(self, permission, user=None, group=None):
        """Return the sql string for granting permissions
        """
        if not atleast_one(user, group):
            raise ValueError('Atleast one of user / group needed')

        result = list()
        option_string = 'WITH GRANT OPTION'
        base = 'GRANT %s ON %s TO {user} {option}' % (
            permission, self.full_name)

        if user is not None:
            result.append(base.format(user=user, option=option_string))

        if group is not None:
            result.append(base.format(user='GROUP %s' % group, option=''))
        return result

    def grant_script(self):
        """Grant the permissions based on the config
        """
        config = Config()
        if not hasattr(config, 'database'):
            return

        permissions = config.database.get('permissions', list())

        sql = list()
        for permission in permissions:
            sql.extend(self._grant_sql_builder(**permission))

        return SqlScript('; '.join(sql))

    def select_script(self):
        """Select everything from the relation
        """
        return SqlScript('SELECT * FROM %s' % self.full_name)

    def create_script(self, grant_permissions=True):
        """Create script for the table object
        """
        script = SqlScript(statements=[self.sql_statement.copy()])
        if grant_permissions:
            script.append(self.grant_script())
        return script

    def recreate_script(self, grant_permissions=True):
        """Sql script to recreate the view
        """
        script = self.drop_script()
        script.append(self.create_script(grant_permissions))
        return script
