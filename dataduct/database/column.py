"""Script containing the column class object
"""


class Column(object):
    """Class representing columns in a table
    """
    def __init__(self, column_name, column_type, encoding=None,
                 fk_reference=None, fk_table=None, is_distkey=False,
                 is_sortkey=False, is_primarykey=False, is_null=False,
                 is_not_null=False, position=None):
        """Constructor for Column class

        Args:
            column_name(str): The name of the column
            column_type(str): The type of the column
            encoding(str): The encoding type of the column
            fk_reference(str): The column that this key is referring to
            fk_table(str): The table that this key is referring to
            is_distkey(bool): Whether or not this column is the DISTKEY
            is_sortkey(bool): Whether or not this column is a SORTKEY
            is_primarykey(bool): Whether or not this column is a primary key
            is_null(bool): Whether or not is column is defaults to null
            is_not_null(bool): Whether or not is column is not nullable
            position(int): The position of the column
        """

        self.column_name = column_name
        self.column_type = column_type
        self.encoding = encoding
        self.fk_reference = fk_reference
        self.fk_table = fk_table
        self.is_distkey = is_distkey
        self.is_sortkey = is_sortkey
        self.is_primarykey = is_primarykey
        self.is_null = is_null
        self.is_not_null = is_not_null
        self.position = position

        if is_null and is_not_null:
            raise ValueError('Column cannot be both NULL and NOT NULL together')  # noqa

        if self.is_primarykey:
            self.is_not_null = True
            self.is_null = False

    def __str__(self):
        """String output for the columns
        """
        if self.column_type is not None:
            return '%s %s' % (self.column_name, self.column_type)
        return self.column_name

    @property
    def primary(self):
        """Property for the column being part of primary key
        """
        return self.is_primarykey

    @primary.setter
    def primary(self, value=True):
        """Set the primary flag for the column
        """
        self.is_primarykey = value

        # Force not null for primary key columns
        if self.is_primarykey:
            self.is_not_null = True
            self.is_null = False

    @property
    def name(self):
        """Get the name of the column
        """
        return self.column_name
