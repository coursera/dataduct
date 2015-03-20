"""
Shared utility functions
"""
from ..parsers import remove_comments
from ..parsers import remove_empty_statements
from ..parsers import split_statements
from ..parsers import remove_transactional
from ..parsers import remove_newlines


def balanced_parenthesis(statement):
    """Check if the SQL statement is balanced
    """
    counter = 0
    for character in statement:
        if character == '(':
            counter += 1
        if character == ')':
            counter -= 1
            if counter < 0:
                return False
    return counter == 0


def sanitize_sql(sql, keep_transaction=False):
    """Sanatize the sql string
    """
    # remove comments
    string = remove_comments(sql)

    # remove transactionals
    if not keep_transaction:
        string = remove_transactional(string)

    # remove new lines
    string = remove_newlines(string)

    # remove empty statements
    string = remove_empty_statements(string)

    # split into multiple statements
    return split_statements(string)
