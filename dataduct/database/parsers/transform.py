"""Module containing basic transform functions on strings
"""

import re

from pyparsing import CaselessKeyword
from pyparsing import CharsNotIn
from pyparsing import Literal
from pyparsing import OneOrMore
from pyparsing import WordStart
from pyparsing import ZeroOrMore
from pyparsing import nestedExpr
from pyparsing import replaceWith


def remove_empty_statements(string, seperator=';'):
    """Remove empty statements from the string

    Args:
        string(str): String to be processed
        seperator(str): Seperater to be checked for duplicates

    Returns:
        result(str): String with empty statements trimmed
    """
    if string == '':
        return string

    empty_statement = seperator + OneOrMore(seperator)
    empty_statement.setParseAction(replaceWith(seperator))
    string = empty_statement.transformString(string)

    return string.lstrip(seperator)


def remove_comments(string):
    """Remove comments from the statements

    Args:
        string(str): String to be processed

    Returns:
        result(str): String with comments trimmed
    """

    if string == '':
        return string

    # Remove multiline comments
    multiline_comment = nestedExpr('/*', '*/').suppress()
    string = multiline_comment.transformString(string)

    # Remove single line comments
    singleline_comment = Literal('--') + ZeroOrMore(CharsNotIn('\n'))
    string = singleline_comment.suppress().transformString(string)

    return string


def remove_transactional(string):
    """Remove begin or commit from the statement

    Args:
        string(str): String to be processed

    Returns:
        result(str): String with begin and commit trimmed
    """
    transaction = WordStart() + (
        CaselessKeyword('BEGIN') | CaselessKeyword('COMMIT'))

    return transaction.suppress().transformString(string)


def split_statements(string, seperator=';', quote_char="'"):
    """Seperate the string based on the seperator

    Args:
        string(str): String to be processed
        seperator(str): Seperater to split the statements

    Returns:
        result(list of str): Statements split based on the seperator
    """
    if string == '':
        return []

    # We can not directly split a sql statement as we want to skip on
    # semicolons inside a string in the sql query.
    stack = 0
    result = []
    statement = ''
    for char in string:
        if char == seperator and not stack % 2:
            result.append(statement.strip())
            statement = ''
        else:
            statement += char
            if char == quote_char:
                stack += 1
    if statement.strip():
        result.append(statement.strip())
    return result


def remove_newlines(string):
    """Remove new lines from a string unless in single quotes
    """
    # In general the aim is to avoid regex as they are hard to maintain
    regex = r"(?:[^\s\n\r']|'(?:\\.|[^'])*')+"
    return ' '.join(re.findall(regex, string))
