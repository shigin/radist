import os
import fcntl
import select

BUFSIZE = 1024*16

class AdvMuxReader(object):
    """Class multiplexes a number of file descriptors in the one
    iterable object by newline.

    It gets list of pairs (file, object), and returns (object, line)
    pair for each line, separated by '\\n'.

    Example:
        fds = [
               (os.popen2('echo -e 1\\n2')[1], 'proc1'),
               (os.popen2('echo -e 3\\n4')[1], 'proc2'),
              ]
        mr = AdvMuxReader(fds)
        for pair in mr:
            print "%s: %s" % pair

    Should print something like:
        proc1: 1
        proc2: 3
        proc1: 2
        proc2: 4
    Iterable returns one line from any file.

    WARNING!!! MuxReader closes files at exit.
    WARNING!!! MuxReader sets files to O_NONBLOCK mode."""
    def __init__(self, filemap):
        "Creates AdvMuxReader object."
        self.__poll = select.poll()
        assert filemap != iter(filemap)
        for xfile, obj in filemap:
            assert 'r' in xfile.mode
        self.__fds = {}
        self.__objs = {}
        self.__buff = {}
        self.__buff[-1] = []
        for xfile, obj in filemap:
            fcntl.fcntl(xfile.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
            self.__poll.register(xfile, select.POLLIN | select.POLLPRI)
            self.__fds[xfile.fileno()] = xfile
            self.__objs[xfile.fileno()] = obj
            self.__buff[xfile.fileno()] = ""

    def __del__(self):
        for fd in self.__fds.values():
            self.__poll.unregister(fd.fileno())

    def __iter__(self):
        return self

    def __unregister_fd(self, fd):
        """Closes file object, deletes it from poll,
        frees buffer.

        Returns the rest of buffer."""
        self.__poll.unregister(fd)
        self.__fds[fd].close()
        del self.__fds[fd]
        result = self.__buff[fd]
        del self.__buff[fd]
        del self.__objs[fd]
        return result

    def __get_buff(self):
        """Returns text from buffer.

        Returns None if buffers are empty."""
        if len(self.__buff[-1]) > 0:
            return self.__buff[-1].pop(0)

        for key, buff in self.__buff.items():
            if key == -1:
                continue
            splited = buff.split('\n', 1)
            if len(splited) > 1:
                self.__buff[key] = splited[1]
                return self.__objs[key], splited[0]
        return None

    def next(self):
        """Returns tuple (object for file, next line)."""
        buff = self.__get_buff()
        if buff is not None:
            return buff

        if len(self.__fds) > 0:
#            if len(self.__fds) < 3:
#                print "FDS: ", self.__fds
            result = []
            for fd, event in self.__poll.poll():
                if event & (select.POLLIN | select.POLLPRI) != 0:
                    x = os.read(fd, BUFSIZE)
                    if len(x) == 0:
                        obj = self.__objs[fd]
                        text = self.__unregister_fd(fd)
                        result.append((obj, text))
                    else:
                        self.__buff[fd] += x
                else:
                    obj = self.__objs[fd]
                    result.append((obj, self.__unregister_fd(fd)))
            self.__buff[-1].extend(result)
            buff = self.__get_buff()
            if buff is None:
                # it's really dirty hack to avoid "Buffers are empty" exception
                return self.next()
            else:
                return buff
        else:
            raise StopIteration

from itertools import repeat, izip
def MuxReader(files):
    """Class multiplexes a number of file descriptors in the one
    iterable object by newline.

    Example:
        fds = [os.popen2('echo -e 1\\n2')[1], os.popen2('echo -e 3\\n4')[1]]
        mr = MuxReader(fds)
        for line in mr:
            print line

    Should print something like:
        1
        3
        2
        4
    Iterable returns one line from any file.

    WARNING!!! MuxReader closes files at exit.
    WARNING!!! MuxReader sets files to O_NONBLOCK mode."""
    filemap = zip(files, repeat(1))
    for obj, line in AdvMuxReader(filemap):
        yield line
