"""Common decorator utilities
"""

import time


def timeit(method):
    """Timing decorator for measuring performance of functions
    """

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print 'Method %r with arguments (%r, %r) took %2.2f secs' % \
              (method.__name__, args, kw, te-ts)
        return result

    return timed
