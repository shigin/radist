import os
import time
import socket
import unittest
from os import statvfs

import radist

CONST = 10
def get_free(path):
    return statvfs(path).f_bfree

def double(n):
    for x  in xrange(n):
        yield x**2

def xd():
    return sum(double(CONST))

def dsum(n):
    return sum(double(n))

def me_local():
    return radist.helpers.is_local(socket.gethostname())

def daver(n):
    return sum([float(x)/dsum(n) for x in double(n)])

def can_read(path):
    return os.access(path, os.R_OK)

def raiser():
    raise OSError

def sleeps():
    for x in xrange(4):
        time.sleep(1)
        yield x

def iter_raise():
    yield int('asd')

r = radist.get_r()
class TestRunner(unittest.TestCase):
    def setUp(self):
        self.node = r.index.c000

    def test_other(self):
        assert self.node.r_pexec(dsum, 10) == dsum(10)

    def test_rec_other(self):
        assert self.node.r_pexec(daver, 10) == daver(10)

    def test_executor(self):
        func = self.node.r_executor(os.getuid, user='-root')
        assert func() == 0

    def test_built_in(self):
        hostname = self.node.r_pexec(socket.gethostname)
        assert self.node.get('server') == hostname

    def test_generator(self):
        assert list(self.node.r_pexec(double, 30)) == list(double(30))

    def test_external(self):
        assert self.node.r_pexec(can_read, '/')

    def test_raise(self):
        self.failUnlessRaises(OSError, self.node.r_pexec, raiser)

    def test_geterator_raise(self):
        gen = self.node.r_pexec(iter_raise)
        self.failUnlessRaises(ValueError, gen.next)

    def test_import_from(self):
        self.node.r_pexec(get_free, '/')

    def test_const(self):
        assert self.node.r_pexec(xd) == xd()

    # XXX doesn't support it yet
    #def test_func_from_module(self):
    #    assert me_local()
    #    assert self.node.r_pexec(me_local)

if __name__ == '__main__':
    unittest.main()
