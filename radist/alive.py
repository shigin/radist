"""Module checks if all servers in node is alive.

WARNING: module raise exception in signal handler. I do not know
if it's good."""
import os
import errno
import signal
import exceptions
from errors import *

__all__ = ['is_alive', 'get_dead_node']

def void_handler(signum, trace):
    """Handler for timeout."""
    raise exceptions.OSError(errno.EINTR, 'Timeout...')

def get_dead_nodes(node, timeout=15, all_nodes=None):
    """Returns dead nodes from the node.
    
    It raises exception if node hasn't got any servers.
    
    Server is alive if it can run hostname without error."""
    assert timeout > 1
    devnull = file(os.devnull, 'r+')
    if all_nodes is None:
        boxes = node.get_servers().get()
    else:
        boxes = node.get_servers(all_nodes=all_nodes).get()
    if len(boxes) == 0:
        raise RadistError("Node '%s' hasn't got any childs." % node.name)
    pids = {}
    for box in boxes:
        pid = box.r_exec('hostname', flags=os.P_NOWAIT, stdin=devnull, \
                stdout=devnull, stderr=devnull)
        pids[pid] = box
    saved = signal.signal(signal.SIGALRM, void_handler)
    signal.alarm(timeout)
    result = []
    try:
        while len(pids) > 0:
            pid, status = os.wait()
            if pid in pids.keys():
                if status != 0:
                    result.append(pids[pid])
                del pids[pid]
            else:
                # FIXME it's a bug
                pass
    except exceptions.OSError, err:
        if err.errno == errno.EINTR:
            # kill all unterminated shit...
            for pid in pids:
                os.kill(pid, signal.SIGTERM)
            for pid in pids:
                os.kill(pid, signal.SIGKILL)
            for pid in pids:
                os.waitpid(pid, os.WNOHANG)
            result.extend(pids.values())

    signal.alarm(0)
    signal.signal(signal.SIGALRM, saved)
    return result

def is_alive(node, **kwargs):
    """Returns True if all servers in node is alive.

    It raises exception if node hasn't got any servers.
    
    See get_dead_nodes."""
    return len(get_dead_nodes(node, **kwargs)) == 0
