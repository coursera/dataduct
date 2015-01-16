"""Create SQL parser
"""
from pyparsing import Group
from pyparsing import printables
from pyparsing import StringEnd
from pyparsing import Word
from pyparsing import ZeroOrMore

from .utils import _create
from .utils import _view
from .utils import _db_name
from .utils import _as

from .helpers import to_dict
from .helpers import replace_check


merge = lambda x: ' '.join(x[0])


def rreplace(s, old, new):
    li = s.rsplit(old, 1)
    return new.join(li)

def parse_create_view(string):
    """Parse the create view sql query and return metadata

    Args:
        string(str): Input sql string that should be parsed

    Returns:
        view_data(dict): view_data dictionary for instantiating a view object
    """

    string = rreplace(string, ')', ' )')

    end = ')' + StringEnd()
    select = Group(ZeroOrMore(~end + Word(printables)))

    parser = _create + replace_check.setResultsName('replace') + _view
    parser += _db_name.setResultsName('view_name') + _as + '('
    parser += select.setParseAction(merge).setResultsName('select_statement')
    parser += end

    # Parse the base table definitions
    view_data = to_dict(parser.parseString(string))

    return view_data
