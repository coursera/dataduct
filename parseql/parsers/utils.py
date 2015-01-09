"""SQL parser utils and constants
"""

from pyparsing import alphanums
from pyparsing import CaselessKeyword
from pyparsing import Combine
from pyparsing import nums
from pyparsing import Optional
from pyparsing import Word


# Functions
isNotEmpty = lambda x: len(x) > 0

# Data types
_smallint = CaselessKeyword('SMALLINT')
_integer = CaselessKeyword('INTEGER')
_bigint = CaselessKeyword('BIGINT')
_decimal = Combine(CaselessKeyword('DECIMAL') + '(' + Word(nums + ',') + ')')
_real = (CaselessKeyword('REAL') | CaselessKeyword('FLOAT'))
_double = CaselessKeyword('DOUBLE')
_boolean = CaselessKeyword('BOOLEAN')
_char = CaselessKeyword('CHAR')
_varchar = Combine(CaselessKeyword('VARCHAR') + '(' + Word(nums) + ')')
_date = CaselessKeyword('DATE')
_timestamp = CaselessKeyword('TIMESTAMP')

# Create SQL keywords
_create = CaselessKeyword('CREATE')
_table = CaselessKeyword('TABLE')
_temp = CaselessKeyword('TEMP')
_temporary = CaselessKeyword('TEMPORARY')
_if_not_exists = CaselessKeyword('IF NOT EXISTS')
_primary_key = CaselessKeyword('PRIMARY KEY')
_foreign_key = CaselessKeyword('FOREIGN KEY')
_references = CaselessKeyword('REFERENCES')
_unique = CaselessKeyword('UNIQUE')
_null = CaselessKeyword('NULL')
_not_null = CaselessKeyword('NOT NULL')
_distkey = CaselessKeyword('DISTKEY')
_diststyle = CaselessKeyword('DISTSTYLE')
_sortkey = CaselessKeyword('SORTKEY')
_encode = CaselessKeyword('ENCODE')
_all = CaselessKeyword('ALL')
_even = CaselessKeyword('EVEN')
_key = CaselessKeyword('KEY')

# Parsers
_db_name = Word(alphanums+"_-.")
temporary_check = Optional(_temp | _temporary).setParseAction(isNotEmpty)
existance_check = Optional(_if_not_exists).setParseAction(isNotEmpty)
pk_check = (_primary_key | _unique)

column_types = _smallint | _integer | _bigint | _decimal | _real | _double
column_types |= _boolean | _char | _varchar | _date | _timestamp
