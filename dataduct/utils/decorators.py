"""Common decorator utilities
"""

from datetime import datetime


def timeit(method):
    """Timing decorator for measuring performance of functions
    """

    def timed(*args, **kw):
        ts = datetime.now()
        print 'Starting time for Method %r is %s' % (method.__name__, ts)

        result = method(*args, **kw)
        te = datetime.now()
        print 'End time for Method %r is %s' % (method.__name__, te)

        print 'Method %r with arguments (%r, %r) took %s' % \
            (method.__name__, args, kw, te - ts)
        return result

    return timed
