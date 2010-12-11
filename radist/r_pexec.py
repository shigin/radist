import pickle
import socket
import inspect
import os, sys
import types
import functools
import __builtin__

import helpers

PRUN = """
import sys, pickle
def loads(s): return pickle.loads(s.decode('hex'))
def dumps(obj): return pickle.dumps(obj).encode('hex')
def debug(s): sys.stderr.write(s)
def out(s):
    sys.stdout.write(s)
    sys.stdout.write("\\n")
    sys.stdout.flush()

module = sys.modules[__name__]
for x in loads('%(modules)s'):
    try: setattr(module, x, __import__(x))
    except: pass
for n, m in loads('%(ifrom)s'):
    setattr(module, n, getattr(__import__(m), n))
%(func)s

args = loads('%(args)s')
kwargs = loads('%(kwargs)s')
try:
    result = %(func_name)s(*args, **kwargs)
    if hasattr(result, '__iter__') and iter(result) == result:
        out("iterator")
        #debug("iterator\\n")
        for x in result:
            import time
            #debug("remote: iterator %%s\\n" %% time.strftime("%%X"))
            out(dumps(x))
    else:
        out("normal")
        out(dumps(result))
except Exception, e:
    #debug("remote exception\\n")
    out(dumps(e))
"""

def dumps(obj):
    return pickle.dumps(obj).encode('hex')

def loads(s):
    return pickle.loads(s.strip().decode('hex'))

def local_exec(func, *args, **kwargs):
    return func(*args, **kwargs)

def executor(server, func, user):
    if helpers.is_local(server):
        if helpers.is_current_user(helpers.get_user(user)):
            return func
    stdin, input = helpers.r_popen2(server, '/usr/bin/env python',
                                    user=user)
    return functools.partial(executor_helper, stdin, input, func)

def executor_helper(stdin, stdout, func, *args, **kwargs):
    r = pexec_helper(stdin, stdout, func, *args, **kwargs)
    if r.next() == "normal":
        return r.next()
    else:
        return r

def r_pexec(server, func, *args, **kwargs):
    return executor(server, func, user=None)(*args, **kwargs)

def func_string_import(func):
    sfunc = inspect.getsource(func)
    modules = sys.modules.keys()
    umodules = {}
    ifrom = {}
    for name in func.func_code.co_names:
        if name in modules:
            umodules[name] = 1
    local = vars(sys.modules[func.__module__])
    for name in func.func_code.co_names:
        if hasattr(__builtin__, name):
            continue
        if name in local:
            if isinstance(local[name], types.FunctionType):
                s, xm, im = func_string_import(local[name])
                sfunc = '\n'.join([s, sfunc])
                for x in xm: umodules[x] = 1
                for x in im: ifrom[x] = 1
                continue
            if isinstance(local[name], types.BuiltinFunctionType):
                ifrom[(name, local[name].__module__)] = 1
                continue
            try:
                sfunc  = '\n'.join([sfunc,
                        "%s = loads('%s')" % (name, dumps(local[name]))
                                  ])
                continue
            except TypeError:
                pass
        if name in umodules:
            continue
        deal = False
        for m in umodules:
            if hasattr(sys.modules[m], name):
                deal = True
                break
                raise Exception("need %s from %s" % (name, m))
        if not deal:
            pass
            #raise Exception("can't deal with %s" % name)
    return sfunc, umodules.keys(), ifrom.keys()

def pexec_helper(stdin, input, func, *args, **kwargs):
    import mux_fds
    input = mux_fds.MuxReader([input])
    uses = []
    if isinstance(func, types.BuiltinFunctionType):
        sfunc = func.__name__ + ' = loads("%s")' % dumps(func)
        modules = []
        ifrom = []
    else:
        sfunc, modules, ifrom = func_string_import(func)
    d = {
        #'func': pickle.dumps(func).encode('hex'),
        'func': sfunc,
        'func_name': func.__name__,

        'ifrom': dumps(ifrom),
        'modules': dumps(modules),
        'args': dumps(args),
        'kwargs': dumps(kwargs)
        }
    #print PRUN % d
    stdin.write(PRUN % d)
    stdin.close()

    #print "???", time.strftime("%X")
    x = input.next().strip()
    #print "what ok", time.strftime("%X")
    if x == "normal":
        yield x
        z = loads(input.next())
        if isinstance(z, Exception):
            raise z
        #print "helper: ", z
        yield z
    elif x == "iterator":
        yield x
        #print "helper... iterator"
        for result in input:
            #print "helper... iterate.."
            if result:
                z = loads(result)
                if isinstance(z, Exception):
                    raise z
                yield z
    else:
        #print "helper... raise"
        raise loads(x)

