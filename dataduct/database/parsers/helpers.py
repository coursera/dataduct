"""SQL parser helpers
"""
from pyparsing import delimitedList
from pyparsing import Optional
from pyparsing import ParseResults

from .utils import _db_name
from .utils import _temp
from .utils import _temporary
from .utils import _if_not_exists
from .utils import _or_replace

# Functions
isNotEmpty = lambda x: len(x) > 0

temporary_check = Optional(_temp | _temporary).setParseAction(isNotEmpty)

replace_check = Optional(_or_replace).setParseAction(isNotEmpty)

existance_check = Optional(_if_not_exists).setParseAction(isNotEmpty)


def paranthesis_list(output_name, input_var=_db_name):
    """Parser for a delimiedList enclosed in paranthesis
    """
    return '(' + delimitedList(input_var).setResultsName(output_name) + ')'


def exists(parser, output_name):
    """Get a parser that returns boolean on existance
    """
    return parser.setParseAction(isNotEmpty).setResultsName(output_name)


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
