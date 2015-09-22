"""Select SQL parser
"""
from pyparsing import MatchFirst
from pyparsing import ParseException
from pyparsing import Word
from pyparsing import WordStart
from pyparsing import delimitedList
from pyparsing import printables
from pyparsing import restOfLine

from .utils import _as
from .utils import _db_name
from .utils import _from
from .utils import _join
from .utils import _select
from .utils import _with
from .utils import field_parser
from .utils import subquery


def deduplicate_with_order(seq):
    """Deduplicate a sequence while preserving the order
    """
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def parse_select_base(string):
    """Parse a select query and return the dependencies

    Args:
        string(str): Input string to be parsed

    Returns:
        result(list of str): List of dependent tables
    """

    if string == '':
        return

    base_parser = _select + restOfLine

    # Sanity check that query starts with select
    base_parser.parseString(string)


def parse_select_dependencies(string):
    """Parse a select query and return the dependencies

    Args:
        string(str): Input string to be parsed

    Returns:
        result(list of str): List of dependent tables
    """

    if string == '':
        return list()

    # Find all dependent tables
    dep_parse = WordStart() + (_from | _join) +\
        _db_name.setResultsName('table')
    output = dep_parse.setParseAction(lambda x: x.table).searchString(string)

    # Flatten the list before returning
    flattened_output = [item for sublist in output for item in sublist]

    # Deduplicated the list
    unique_output = deduplicate_with_order(flattened_output)

    if len(unique_output) == 0:
        raise ParseException('No dependent table in select query')
    return unique_output


def parse_select_columns(string):
    """Parse a select query and return the columns

    Args:
        string(str): Input string to be parsed

    Returns:
        result(list of str): List of columns
    """

    if string == '':
        return list()

    if string.upper().startswith('WITH'):
        suppressor = _with + delimitedList(_db_name + _as + subquery)
        string = suppressor.suppress().transformString(string)

    # Supress everything after the first from
    suppressor = MatchFirst(_from) + restOfLine
    string = suppressor.suppress().transformString(string)

    parser = _select + delimitedList(field_parser).setResultsName('columns')
    output = parser.parseString(string).columns.asList()

    # Strip extra whitespace from the string
    return [column.strip() for column in output]


def parse_column_name(string):
    """Parse column name from select query

    Note:
        This assumes that every column has a name and is the last word of str

    Args:
        string(str): Input string to be parsed

    Returns:
        result(str): column name
    """
    # Find all words in the string
    words = Word(printables.replace('\n\r', '')).searchString(string)

    # Get the last word matched
    # TODO: Make it more complicated
    name = words.pop().asList().pop()
    return name
