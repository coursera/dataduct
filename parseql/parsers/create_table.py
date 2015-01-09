"""Create SQL parser
"""
from pyparsing import alphanums
from pyparsing import CharsNotIn
from pyparsing import Combine
from pyparsing import delimitedList
from pyparsing import OneOrMore
from pyparsing import ParseException
from pyparsing import ParseResults
from pyparsing import Word
from pyparsing import ZeroOrMore

from ..sql.sql_statement import SqlStatement

from .utils import _all
from .utils import _create
from .utils import _db_name
from .utils import _distkey
from .utils import _diststyle
from .utils import _encode
from .utils import _even
from .utils import _foreign_key
from .utils import _key
from .utils import _not_null
from .utils import _null
from .utils import _references
from .utils import _sortkey
from .utils import _table
from .utils import column_types
from .utils import existance_check
from .utils import isNotEmpty
from .utils import pk_check
from .utils import temporary_check


def paranthesis_list(output_name, input_var=_db_name):
    """Parser for a delimiedList enclosed in paranthesis
    """
    return '(' + delimitedList(input_var).setResultsName(output_name) + ')'


def fk_reference():
    """Get Parser for foreign key references
    """
    fk_reference_columns = paranthesis_list('fk_reference_columns')
    fk_table = _db_name.setResultsName('fk_table')
    return _references + fk_table + fk_reference_columns


def exists(parser, output_name):
    """Get a parser that returns boolean on existance
    """
    return parser.setParseAction(isNotEmpty).setResultsName(output_name)


def get_base_parser():
    """Get a pyparsing parser for a create table statement

    Returns:
        table_definition(pyparsing): Parser for create table statements
    """

    temp_check = temporary_check.setResultsName('temporary')
    exists_check = existance_check.setResultsName('if_exists')

    table_name = _db_name.setResultsName('table_name')

    # Initial portions of the table definition
    def_start = _create + temp_check + _table + table_name + exists_check

    subquery = Combine('(' + ZeroOrMore(CharsNotIn(')')) + ')')
    _word = Word(alphanums+"_-. ")
    def_field = Combine(OneOrMore(_word | subquery))

    table_def = def_start + paranthesis_list('raw_fields', def_field) + \
                get_attributes_parser()

    return table_def


def get_column_parser():
    """Get a pyparsing parser for a create table column field statement

    Returns:
        column_definition(pyparsing): Parser for column definitions
    """
    column_name = _db_name.setResultsName('column_name')
    column_type = column_types.setResultsName('column_type')

    constraints = exists(_not_null, 'is_not_null')
    constraints |= exists(_null, 'is_null')
    constraints |= exists(pk_check, 'is_primary_key')
    constraints |= exists(_distkey, 'is_distkey')
    constraints |= exists(_sortkey, 'is_sortkey')
    constraints |= fk_reference()
    constraints |= _encode + _db_name.setResultsName('encoding')

    column_def = column_name + column_type + ZeroOrMore(constraints)
    return column_def


def get_constraints_parser():
    """Get a pyparsing parser for a create table constraints field statement

    Returns:
        constraints_definition(pyparsing): Parser for constraints definitions
    """
    # Primary Key Constraints
    def_pk = pk_check + paranthesis_list('pk_columns')

    # Foreign Key Constraints
    def_fk = _foreign_key + paranthesis_list('fk_columns') + fk_reference()

    return def_pk | def_fk


def get_attributes_parser():
    """Get a pyparsing parser for a create table attributes

    Returns:
        attribute_parser(pyparsing): Parser for attribute definitions
    """
    diststyle_def = _diststyle + (_all | _even | _key).setResultsName(
        'diststyle')

    distkey_def = _distkey + paranthesis_list('distkey')
    sortkey_def = _sortkey + paranthesis_list('sortkey')

    return OneOrMore(diststyle_def | sortkey_def | distkey_def)


def to_dict(input):
    """Purge the ParseResults from output dictionary
    """
    output = dict()
    for key, value in input.asDict().iteritems():
        if isinstance(value, ParseResults):
            output[key] = value.asList()
        else:
            output[key] = value

    return output


def parse_create_table(statement):
    """Parse the create table sql query and return metadata

    Args:
        statement(SqlStatement): Input sql statement that should be parsed

    Returns:
        table_data(dict): table_data dictionary for instantiating a table object
    """

    if not isinstance(statement, SqlStatement):
        raise ValueError('Input to table parser must of a SqlStatement object')

    string = statement.sql()

    # Parse the base table definitions
    table_data = to_dict(get_base_parser().parseString(string))

    # Parse the columns and append to the list
    table_data['columns'] = list()
    table_data['constraints'] = list()

    for field in table_data['raw_fields']:
        try:
            column = to_dict(get_column_parser().parseString(field))
            table_data['columns'].append(column)
        except ParseException:
            try:
                constraint = to_dict(
                    get_constraints_parser().parseString(field))
                table_data['constraints'].append(constraint)
            except ParseException:
                print '[Error] : ', field
                raise

    return table_data
