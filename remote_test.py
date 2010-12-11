#!/usr/bin/env python
"""The test which rely on remote machines should live here."""
import socket
import unittest
import radist

class Fucks(unittest.TestCase):
    def setUp(self):
        self.test = radist.IXConfig(config=socket.gethostname() + " -test\nindex1.rambler.ru -test").test

    def tearDown(self):
        del self.test

    def test_double_apply_not_one(self):
        assert self.test.cluster_exec('echo %% > /dev/null', parallel=True)[0][1] == 0
        assert self.test.cluster_exec('echo %% > /dev/null', single=True)[0][1] == 0

    def test_r_exec(self):
        assert self.test.c000.r_exec('echo %% > /dev/null') == 0

    def test_r_exec_with_sudo(self):
        assert self.test.c001.r_exec('cd /tmp > /dev/null', user='-root') == 0
        stdin, stdout, stderr = self.test.c001.r_popen3('cd /tmp 2>/dev/null && echo ok || echo no', user='-root')
        stdin.close()
        odata = stdout.read().strip()
        edata = stderr.read().strip()
        stdout.close()
        stderr.close()
        assert len(edata) == 0, edata
        assert odata == "ok"

class Alive(unittest.TestCase):
    def test_alive(self):
        clu = self.get_clu(('10.1.1.1', 'index3.rambler.ru', 'index1.rambler.ru'))
        assert not radist.is_alive(clu.test, timeout=4)

    def test_cluster_dead(self):
        clu = self.get_clu(('10.1.1.1', 'index3.rambler.ru', 'index1.rambler.ru'))
        self.failUnlessRaises(radist.RadistError, clu.test.cluster_exec, 'echo %(server)s > /dev/null', check=True)

    def test_cluster_alive(self):
        clu = self.get_clu(('index10.rambler.ru', 'index3.rambler.ru', 'index1.rambler.ru'))
        result = clu.test.cluster_exec('echo %(server)s > /dev/null', check=True)
        for node, status in result:
            assert status == 0

    def test_good_check(self):
        clu = self.get_clu(('index3.rambler.ru', 'index1.rambler.ru'))
        assert radist.is_alive(clu.test, timeout=4)
        result = clu.test.cluster_exec('echo %(server)s > /dev/null', check=True)
        for node, status in result:
            assert status == 0

    def test_get_dead(self):
        clu = self.get_clu(('10.1.1.1', 'index3.rambler.ru', 'index1.rambler.ru'))
        deads = radist.get_dead_nodes(clu.test, timeout=4)
        assert len(deads) == 1
        assert deads[0].primary.server == '10.1.1.1'

    def test_get_dead_dns(self):
        clu = self.get_clu(('absent-name-in-dns.rambler.ru', 'index3.rambler.ru', 'index1.rambler.ru'))
        deads = radist.get_dead_nodes(clu.test, timeout=4)
        assert len(deads) == 1
        assert deads[0].primary.server == 'absent-name-in-dns.rambler.ru'

    def test_zero_serv(self):
        clu = self.get_clu(('10.1.1.1', 'index3.rambler.ru', 'index1.rambler.ru'))
        self.failUnlessRaises(radist.RadistError, radist.is_alive, clu)

    def get_clu(self, clus):
        return radist.IXConfig(config=['%s -test' % server for server in clus])


if __name__ == '__main__':
    unittest.main()
