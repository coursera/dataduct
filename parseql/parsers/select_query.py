"""Select SQL parser
"""
from pyparsing import restOfLine
from pyparsing import MatchFirst
from pyparsing import delimitedList
from pyparsing import WordStart

from ..sql.sql_statement import SqlStatement

from .utils import _db_name
from .utils import _from
from .utils import _join
from .utils import _select
from .utils import def_field


def parse_select_base(statement):
    """Parse a select query and return the dependencies

    Args:
        statement(SqlStatement): Input sql statement that should be parsed

    Returns:
        result(list of str): List of dependent tables
    """

    if not isinstance(statement, SqlStatement):
        raise ValueError('Input to table parser must of a SqlStatement object')

    string = statement.sql()

    if string == '':
        return

    base_parser = _select + restOfLine

    # Sanity check that query starts with select
    base_parser.parseString(string)


def parse_select_dependencies(statement):
    """Parse a select query and return the dependencies

    Args:
        statement(SqlStatement): Input sql statement that should be parsed

    Returns:
        result(list of str): List of dependent tables
    """

    if not isinstance(statement, SqlStatement):
        raise ValueError('Input to table parser must of a SqlStatement object')

    string = statement.sql()

    if string == '':
        return list()

    # Find all dependent tables
    dep_parse = WordStart() + (_from | _join) + _db_name.setResultsName('table')
    output = dep_parse.setParseAction(lambda x: x.table).searchString(string)

    # Flatten the list before returning
    flattened_output = [item for sublist in output for item in sublist]

    # Deduplicated the list
    return list(set(flattened_output))


def parse_select_columns(statement):
    """Parse a select query and return the columns

    Args:
        statement(SqlStatement): Input sql statement that should be parsed

    Returns:
        result(list of str): List of columns
    """

    if not isinstance(statement, SqlStatement):
        raise ValueError('Input to table parser must of a SqlStatement object')

    string = statement.sql()

    if string == '':
        return list()

    # Supress everything after the first from
    suppressor = MatchFirst(_from) + restOfLine
    string = suppressor.suppress().transformString(string)

    parser = _select + delimitedList(def_field).setResultsName('columns')
    output = parser.parseString(string).columns.asList()

    # Strip extra whitespace from the string
    return [column.strip() for column in output]
