"""Module containing basic transform functions on strings
"""

import re

from pyparsing import CaselessKeyword
from pyparsing import CharsNotIn
from pyparsing import delimitedList
from pyparsing import Literal
from pyparsing import nestedExpr
from pyparsing import OneOrMore
from pyparsing import originalTextFor
from pyparsing import printables
from pyparsing import replaceWith
from pyparsing import Word
from pyparsing import ZeroOrMore


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
    singleline_comment = Literal('--') +  ZeroOrMore(CharsNotIn('\n'))
    string = singleline_comment.suppress().transformString(string)

    return string


def remove_transactional(string):
    """Remove begin or commit from the statement

    Args:
        string(str): String to be processed

    Returns:
        result(str): String with begin and commit trimmed
    """
    transaction = (CaselessKeyword('BEGIN')| CaselessKeyword('COMMIT'))
    return transaction.suppress().transformString(string)


def split_statements(string, seperator=';'):
    """Seperate the string based on the seperator

    Args:
        string(str): String to be processed
        seperator(str): Seperater to split the statements

    Returns:
        result(list of str): Statements split based on the seperator
    """
    if string == '':
        return []

    # words can contain anything but the seperator
    printables_less_seperator = printables.replace(seperator, '')

    # capture content between seperators, and preserve original text
    content = originalTextFor(OneOrMore(Word(printables_less_seperator)))

    # process the string
    tokens = delimitedList(content, seperator).parseString(string)

    return tokens.asList()


def remove_newlines(string):
    """Remove new lines from a string unless in single quotes
    """
    # In general the aim is to avoid regex as they are hard to maintain
    regex = r"(?:[^\s\n\r']|'(?:\\.|[^'])*')+"
    return ' '.join(re.findall(regex, string))
