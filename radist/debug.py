import os
import sys
import time
import itertools
try:
    from functools import update_wrapper
except ImportError:
    # python < 2.5
    def update_wrapper(wrapper, wrapped):
        for attr in ('__module__', '__name__', '__doc__'):
            setattr(wrapper, attr, getattr(wrapped, attr))
        wrapper.__dict__.update(wrapped.__dict__)
        return wrapper

if 'PYRADIST_FILE' in os.environ: 
    output = open(os.environ['PYRADIST_FILE'], 'w')
else:
    output = sys.stderr
sequence = itertools.count()

class Proxy(object):
    def __init__(self, friend, callback=None):
        self.callback = callback
        self.friend = friend

    def __setattr__(self, name, value):
        if name in ('friend', 'callback'):
            object.__setattr__(self, name, value)
        else:
            setattr(self.friend, name, value)

    def __getattribute__(self, name):
        if name in ('friend', 'callback'):
            return object.__getattribute__(self, name)
        else:
            return getattr(self.friend, name)

    def __del__(self):
        if self.callback:
            self.callback(self.friend)

def debug_decorator(func):
    def nested(*arg, **kwarg):
        start = time.time()
        num = sequence.next()
        output.write('radist [%d] func: %s with args %s %s\n' % \
                (num, func.__name__, arg, kwarg))
        result = func(*arg, **kwarg)
        output.write('radist [%d] time: %d s\n' % (num, int(time.time() - start)))
        cb = lambda obj: output.write('radist [%d] "%s" time: %d s\n' % (num, repr(obj), int(time.time() - start)))
        if hasattr(result, '__iter__'):
            return map(lambda x: Proxy(x, cb), result)
        else:
            return Proxy(result, cb)

    update_wrapper(nested, func)
    return nested

def debug_mdecorator(func):
    def nested(*arg, **kwarg):
        output.write('radist func: %s with args %s %s\n' % \
                (func.__name__, arg[1:], kwarg))
        return func(*arg, **kwarg)

    update_wrapper(nested, func)
    return nested
