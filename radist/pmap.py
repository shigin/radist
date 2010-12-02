import Queue
from itertools import count
import threading

debug = lambda x: x
try:
    import thread2
    Thread = thread2.Thread
except ImportError:
    Thread = threading.Thread
 
class PMapThread(Thread):
    """PMapThread is a helper for pmap function.

    It gets tuple (num, args, kwargs) from in_queue, 
    calls function func(*args, **kwargs) and put
    tuple (num, flag, result) to out_queue.
    Normally flag is set to True.

    If there is no more tuples in in_queue, returns 
    (-1, False, self).

    If func raise an error, tuple (num, False, exception)
    """
    def __init__(self, in_queue, out_queue, func):
        """Make new PMapThread."""
        threading.Thread.__init__(self)
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        while True:
            try:
                number, args, kwargs = self.in_queue.get_nowait()
                debug("get %d task" % number)
            except Queue.Empty:
                self.out_queue.put((-1, False, self))
                return 
            try:
                result = self.func(*args, **kwargs)
                self.out_queue.put((number, True, result))
            except SystemExit:
                break
            except Exception, err:
                # XXX
                self.out_queue.put((number, False, err))

def _empty_queue(queue):
    try:
        while True:
            queue.get_nowait()
    except Queue.Empty:
        pass
        
def _pmap(func, args, max_threads=10, extra={}, ignore_exceptions=False):
    """See pmap"""
    thread_pool = {}
    in_queue = Queue.Queue()
    out_queue = Queue.Queue()
    number = count()
    for _ in xrange(max_threads):
        thread_pool[PMapThread(in_queue, out_queue, func)] = 1

    if len(args) == 1:
        for arg in args[0]:
            in_queue.put([number.next(), (arg, ), extra])
    else:
        for arg in map(None, *args):
            debug("add elem: %s" % repr(args))
            in_queue.put([number.next(), arg, extra])

    for thread in thread_pool:
        thread.setDaemon(True)
        thread.start()
    next_num = 0
    cache = {}
    do_wait = True
    while True:
        try:
            num, flag, result = out_queue.get(do_wait)
        except Queue.Empty:
            while len(cache) > 0:
                debug("eject old_cache")
                yield cache.pop(next_num)
                next_num += 1
            break
        if num == -1:
            debug("eject thread")
            result.join()
            del thread_pool[result]
            if len(thread_pool) == 0:
                do_wait = False
        else:
            debug("get 'in' %d" % num)
            if next_num == num:
                debug("eject result")
                next_num += 1
                if flag:
                    yield result
                else: 
                    if not ignore_exceptions: 
                        _empty_queue(in_queue)
                        raise result
            else:
                debug("store result")
                cache[num] = result
                if next_num in cache:
                    debug("eject cached")
                    yield cache.pop(next_num)
                    next_num += 1

def _thread_helper(queue, iterable):
    try:
        for i in iterable:
            queue.put([False, i])
        queue.put([True, None])
    except Exception, err:
        queue.put([True, err])

def _queue_reader(queue):
    while True:
        end, result = queue.get()
        if end:
            if result is None:
                break
            else:
                raise result
        yield result

def iter_in_thread(iterator):
    queue = Queue.Queue()
    threading.Thread(target=_thread_helper, args=(queue, iterator)).start()
    return _queue_reader(queue)

def pmap(func, *args, **kwargs):
    """Works like map, but make a thread for each item in sequence.

    Warning! If func is None, map will be called.
    Extra flags:
        max_threads [10] --- maximum worker thread.
        extra [{}]       --- extra argument to function (kwargs), i. e.
                pmap(long_calc, extra={"sleep": 3})
        ignore_exceptions [False] --- if the option is set to True,
            exception any exception in func will be ignored, i.e.
                pmap(int, [1, 2, '23'], ignore_exceptions=True) -> [1, 2]

    Example:
    >>> iterable = pmap(long_calc, range(10))
    """
    if func is None:
        return map(None, *args)
    if len(args) == 0:
        raise TypeError("pmap() requires at least two args")
    return _pmap(func, args, **kwargs)
