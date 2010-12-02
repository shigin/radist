"""Module contain common methods and objects to work with radist."""
from parser import RadistConf
from basenode import RadistNode, FakeNode, get_node, local_node
from extnode import ColoredNode
from helpers import r_exec, r_popen3, r_popen2, R_SUB2, R_SUB3
from errors import *
from ix import IXConfig
from alive import is_alive, get_dead_nodes
from mux_fds import MuxReader, AdvMuxReader
from default import get_r, get_ix
from content import *
from pmap import pmap
from other import cluster_read

def _set_debug():
    import os, sys
    def is_wrap(module, name):
        thing = getattr(module, name)
        ca = not name.startswith('_') and callable(thing) 
        if ca:
            return type(thing) != type(Exception) or not issubclass(thing, Exception)
        else:
            return False

    if 'PYRADIST_DEBUG' in os.environ:
        from debug import debug_decorator, debug_mdecorator
        sys.stderr.write("radist: debug mode enabled, unset PYRADIST_DEBUG if you don't want it\n")
        module = sys.modules[__name__]
        to_debug = os.environ['PYRADIST_DEBUG']
        if to_debug.isdigit():
            for attr in [name for name in dir(module) if is_wrap(module, name)]:
                # sys.stderr.write("wrap %s\n" % attr)
                thing = getattr(module, attr)
                if hasattr(thing, '__init__'):
                    for method in [x for x in dir(thing) if is_wrap(thing, x)]:
                        sys.stderr.write("wrap %s.%s\n" % (attr, method))
                        setattr(thing, method, debug_mdecorator(getattr(thing, method)))
                else:
                    setattr(module, attr, debug_decorator(thing))
        else:
            for attr in to_debug.split():
                path = attr.split('.')
                x = module
                while len(path) > 0:
                    old = x
                    name = path.pop(0)
                    x = getattr(x, name, None)
                if x != None:
                    # XXX this can be bad :(
                    if type(x) == type(RadistNode.__init__):
                        setattr(old, name, debug_mdecorator(x))
                    else:
                        setattr(old, name, debug_decorator(x))
                else:
                    sys.stderr.write("radist: can't find '%s' method\n" % attr)

_set_debug()
del _set_debug
