"""SQL parser utils and constants
"""

from pyparsing import alphanums
from pyparsing import CaselessKeyword
from pyparsing import Combine
from pyparsing import Forward
from pyparsing import nums
from pyparsing import OneOrMore
from pyparsing import Word


# Data types
_smallint = CaselessKeyword('SMALLINT')
_integer = CaselessKeyword('INTEGER')
_bigint = CaselessKeyword('BIGINT')
_decimal = Combine(CaselessKeyword('DECIMAL') + '(' + Word(nums + ',') + ')')
_real = (CaselessKeyword('REAL') | CaselessKeyword('FLOAT'))
_double = CaselessKeyword('DOUBLE')
_boolean = CaselessKeyword('BOOLEAN')
_char = CaselessKeyword('CHAR')
_varchar = Combine(CaselessKeyword('VARCHAR') + '(' + Word(alphanums) + ')')
_date = CaselessKeyword('DATE')
_timestamp = CaselessKeyword('TIMESTAMP')

# Create SQL keywords
_create = CaselessKeyword('CREATE')
_table = CaselessKeyword('TABLE')
_view = CaselessKeyword('VIEW')
_temp = CaselessKeyword('TEMP')
_temporary = CaselessKeyword('TEMPORARY')
_if_not_exists = CaselessKeyword('IF NOT EXISTS')
_or_replace = CaselessKeyword('OR REPLACE')
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

# Select SQL Keywords
_select = CaselessKeyword('SELECT')
_from = CaselessKeyword('FROM')
_as = CaselessKeyword('AS')
_join = CaselessKeyword('JOIN')

# Parsers
_db_name = Word(alphanums+"_-.")
pk_check = (_primary_key | _unique)

# Column types
column_types = _smallint | _integer | _bigint | _decimal | _real | _double
column_types |= _boolean | _char | _varchar | _date | _timestamp

# Define a field parser for create table fields or select query fields
field_parser = Forward()
subquery = Forward()

# List of characters allowed in the query statements
special_character = "_-. *`><!+/=%':{}"
_word = Word(alphanums + special_character)

# Subqueries allow words and commas in them
_word_subquery = Word(alphanums + "," + special_character)

# Field / Subquery are either one or more words or subquery
field_parser << Combine(OneOrMore(_word | subquery))
subquery << Combine('(' + Combine(OneOrMore(_word_subquery | subquery)) + ')')
