"""
Shared utility functions
"""
import time
import math
from sys import stderr


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
