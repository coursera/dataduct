"""
Shared utility functions
"""
import time
import math
import os
from sys import stderr

from ..config import Config

RESOURCE_BASE_PATH = 'RESOURCE_BASE_PATH'
CUSTOM_STEPS_PATH = 'CUSTOM_STEPS_PATH'


def atmost_one(*args):
    """Asserts one of the arguments is not None

    Returns:
        result(bool): True if exactly one of the arguments is not None
    """
    return sum([1 for a in args if a is not None]) <= 1


def atleast_one(*args):
    """Asserts one of the arguments is not None

    Returns:
        result(bool): True if atleast one of the arguments is not None
    """
    return sum([1 for a in args if a is not None]) >= 1


def exactly_one(*args):
    """Asserts one of the arguments is not None

    Returns:
        result(bool): True if exactly one of the arguments is not None
    """
    return sum([1 for a in args if a is not None]) == 1


def retry(tries, delay=3, backoff=2):
    """Retries a function or method until it succedes

    Note:
        This assume the function succeded if no exception was thrown

    Args:
        tries(int): Number of attempts of the function. Must be >= 0
        delay(int): Initial delay in seconds, should be > 0
        backoff(int): Factor by which delay should increase between attempts
    """

    if backoff <= 1:
        raise ValueError('backoff must be greater than 1')

    tries = math.floor(tries)
    if tries < 0:
        raise ValueError('tries must be 0 or greater')

    if delay <= 0:
        raise ValueError('delay must be greater than 0')

    def deco_retry(f):
        """Decorator for retries"""

        def function_attempt(f, *args, **kwargs):
            """
            Single attempt of the function
            """
            template = 'Attempt failed with Exception: \n{0}: {1}\n'
            try:
                r_value = f(*args, **kwargs) # first attempt
                r_status = True
            except Exception as exp:
                stderr.write(template.format(type(exp).__name__, exp))
                r_value = exp
                r_status = False

            return r_value, r_status

        def f_retry(*args, **kwargs):
            """True decorator"""
            m_tries, m_delay = tries, delay # make mutable

            r_value, r_status = function_attempt(f, *args, **kwargs)

            while m_tries > 0:

                # Done on success
                if r_status is True:
                    return r_value

                m_tries -= 1        # consume an attempt
                time.sleep(m_delay) # wait...
                m_delay *= backoff  # make future wait longer

                # Try again
                r_value, r_status = function_attempt(f, *args, **kwargs)

            if r_status is True:
                return r_value
            else:
                raise r_value

        # true decorator -> decorated function
        return f_retry

    # @retry(arg[, ...]) -> true decorator
    return deco_retry


def parse_path(path, path_type=RESOURCE_BASE_PATH):
    """Change the resource paths for files and directory based on params

    If the path is None, the function returns None.
    Else if the path is an absolute path then return the path as is.
    Else if the path is a relative path and resource_base_path is declared then
        assume the path is relative to the resource_base_path
    Else return the path as is.

    Args:
        path(str): path specified in the YAML file
    """
    # If path is None or absolute
    if path is None or os.path.isabs(path):
        return path

    # Try relative path to specified config
    config = Config()
    if path_type == RESOURCE_BASE_PATH:
        if RESOURCE_BASE_PATH in config.etl:
            return os.path.join(config.etl[RESOURCE_BASE_PATH], path)
    else:
        if CUSTOM_STEPS_PATH in config.etl:
            return os.path.join(config.etl[CUSTOM_STEPS_PATH], path)

    # Return the path as is.
    return path
