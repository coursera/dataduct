"""Script containing the history table class object
Child of the table class object
"""

from .table import Table
from .sql import SqlScript
from .select_statement import SelectStatement

HIST_EFFECTIVE_COLUMN = 'effective_ts'
HIST_EXPIRATION_COLUMN = 'expiration_ts'
HIST_EXPIRATION_MAX = '9999-12-31 23:59:59.999999'


class HistoryTable(Table):
    """A history table is a table specifically designed to represent
    Slowly Changing Dimensions
    (http://en.wikipedia.org/wiki/Slowly_changing_dimension).

    Its first two columns must be an effective timestamp and an expiration
    timestamp, but otherwise it looks just like a regular table.
    """

    def __init__(self, sql):
        """Constructor for the HistoryTable class
        """
        super(HistoryTable, self).__init__(sql)
        # Check that first column is the effective timestamp
        # And the second column is the expiration timestamp
        if self.column(HIST_EFFECTIVE_COLUMN) is None or\
                self.column(HIST_EXPIRATION_COLUMN) is None:
            raise ValueError('History table must have effective and expiration'
                             ' timestamps')

    def _select_current_script(self):
        """SQL script to select current view of table
        """

        # Get all columns except for the two timestamps
        selected_columns = [c.name for c in self.columns()
                            if c.name != HIST_EFFECTIVE_COLUMN and
                            c.name != HIST_EXPIRATION_COLUMN]

        return SelectStatement("""
            SELECT {selected_columns}
            FROM {history_name}
            WHERE {expiration_column} = '{expiration_max}'
            """.format(selected_columns=', '.join(selected_columns),
                       history_name=self.full_name,
                       expiration_column=HIST_EXPIRATION_COLUMN,
                       expiration_max=HIST_EXPIRATION_MAX))

    def _expire_history_script(self, source):
        """SQL script to expire outdated records

        Args:
          source (Table): The source from which to update history

        Returns:
          SqlScript: a SQL statement that removes outdated records

        A history row will be expired if:
            It is currently unexpired (expiration timestamp is at max); and
            either:
                It's corresponding row in the source table has been changed; or
                It's corresponding row in the source table has been deleted.
        """

        if not isinstance(source, Table):
            raise ValueError('Source must be a table')

        # Get the secondary columns of the table
        secondary_columns = [column for column in source.columns()
                             if not column.primary]

        # There must be at least one primary and secondary key
        if len(source.primary_keys) == 0:
            raise ValueError('Source table must have a primary key')
        if len(secondary_columns) == 0:
            raise ValueError('Source table must have a non-primary column')

        # Expire if corresponding row in the source table has been changed
        # First, match primary key info to determine corresponding rows
        same_statement =\
            '{history_name}.{column_name} = {source_name}.{column_name}'
        matching_primary_keys_condition = ' AND '.join(
            [same_statement.format(history_name=self.full_name,
                                   source_name=source.full_name,
                                   column_name=column.name)
             for column in source.primary_keys]
        )
        # Then, filter to get only the records that have changed
        # A record has been changed if one of it's non-primary columns
        # are different
        different_statement = """
            {history_name}.{column_name} != {source_name}.{column_name}
            OR (
                {history_name}.{column_name} IS NULL
                AND {source_name}.{column_name} IS NOT NULL
            )
            OR (
                {history_name}.{column_name} IS NOT NULL
                AND {source_name}.{column_name} IS NULL
            )
            """
        record_changed_condition = '(' + ' OR '.join(
            [different_statement.format(history_name=self.full_name,
                                        source_name=source.full_name,
                                        column_name=column.name)
             for column in secondary_columns]
        ) + ')'
        # Lastly, filter to get only the non-expired columns
        # This statement will be reused for the removal check
        not_expired_condition =\
            '{expiration_column} = \'{expiration_max}\''.format(
                expiration_column=HIST_EXPIRATION_COLUMN,
                expiration_max=HIST_EXPIRATION_MAX,
            )
        # Expire changed columns
        script = SqlScript("""
            UPDATE {history_name}
                SET {expiration_column} = SYSDATE - INTERVAL '0.000001 seconds'
            FROM {source_name}
            WHERE {matching_primary_keys}
                AND {record_changed}
                AND {not_expired};
            """.format(history_name=self.full_name,
                       expiration_column=HIST_EXPIRATION_COLUMN,
                       source_name=source.full_name,
                       matching_primary_keys=matching_primary_keys_condition,
                       record_changed=record_changed_condition,
                       not_expired=not_expired_condition))

        # Expire if corresponding row in the source table has been deleted
        # Filter to get the history rows which have primary keys
        # that are no longer in the source table
        primary_keys = ",".join([name for name in source.primary_key_names])
        missing_primary_keys_condition = """
            (
                {primary_keys}
            )
            NOT IN (
                SELECT {primary_keys}
                FROM {source_name}
            )
            """.format(primary_keys=primary_keys,
                       source_name=source.full_name)

        script.append("""
            UPDATE {history_name}
                SET {expiration_column} = SYSDATE - INTERVAL '0.000001 seconds'
            WHERE {missing_primary_keys}
                AND {not_expired};
            """.format(history_name=self.full_name,
                       expiration_column=HIST_EXPIRATION_COLUMN,
                       missing_primary_keys=missing_primary_keys_condition,
                       not_expired=not_expired_condition))
        return script

    def update_history_script(self, source):
        """SQL script to update the history table

        Args:
          source (Table): The source from which to update history

        Returns:
          SqlScript: a SQL statement that updates history

        Raises:
          ValueError: If source is not a Table object
        """

        if not isinstance(source, Table):
            raise ValueError('Source must be a table')

        # Create a temporary copy of the source relation as another table
        temp_table = Table(source.temporary_clone_script())
        result = temp_table.create_script(grant_permissions=False)

        # Insert the values of the original table into the temp table
        result.append(temp_table.insert_script(source))

        # Expire outdated records
        result.append(self._expire_history_script(source))

        # Delete records from the temp table that have not changed
        result.append(
            temp_table.delete_matching_rows_script(
                self._select_current_script()))

        # Insert the remaining rows into destination
        select_statement = SelectStatement("""
            SELECT SYSDATE, '{expiration_max}'::TIMESTAMP, {columns}
            FROM {temp_table_name}
            """.format(expiration_max=HIST_EXPIRATION_MAX,
                       columns=', '.join(
                           [c.name for c in temp_table.columns()]),
                       temp_table_name=temp_table.full_name))
        result.append(self.insert_script(select_statement))

        # Drop the temp table, in case the temporary flag isn't enough
        result.append(temp_table.drop_script())
        return result
