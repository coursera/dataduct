"""
Exceptions for etl_lib
"""


class ETLInputError(Exception):
    """Error raised when function input is incorrect.

    Args:
      msg (str): Human readable string describing the exception.
      code (int, optional): Error code, defaults to 2.

    Attributes:
      msg (str): Human readable string describing the exception.
      code (int): Exception error code.

    """
    def __init__(self, msg, code=2):
        self.msg = msg
        self.code = code
