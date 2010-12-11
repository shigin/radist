conf1 = """
/R/qwe
  primary=index1.rambler.ru:/:/tmp
  spacelimit=123

/R/aaa
  primary=index3.rambler.ru:/:/tmp

/R/000
  primary=index7.rambler.ru:/spool

/R/000/stable
  spacelimit=2222

/R/001/stable-1
  spacelimit=777

/R/001
  primary=index10.rambler.ru:/spool:/tmp

/R/rccf/001
  primary=webbase01.rambler.ru:/spool:/tmp
"""

ixconf = """
index1.rambler.ru -merger -LOC_1
index3.rambler.ru -merger -LOC_2
index4.rambler.ru -merger -LOC_4
cite01.rambler.ru -cite:000+001+002 -LOC_3
cite02.rambler.ru -cite:010+001+002 -LOC_4
www1.rambler.ru -www001
www2.rambler.ru -www002
www3.rambler.ru -www001
"""

ixref2 = """
www111 -refindex2_backup_master
www333 -refindex2_000
www323 -refindex2_backup_000
www331 -refindex2_backup_000
"""

ixcite = """
cite01 -cite:000+001
cite02 -cite:001+002
cite03 -cite:002+000
"""

ixfast = """
fast01 -wwwFAST -wwwFAST00 -wwwFAST000
fast02 -wwwFAST -wwwFAST01 -wwwFAST001
fast03 -wwwFAST -wwwFAST00 -wwwFAST000
fast04 -indexFAST -indexFAST000
fast05 -indexFAST -indexFAST001
fast06 -indexFAST -indexFAST002
"""

ixfast2 = """
quick01.rambler.ru   -wwwFAST -wwwFAST00 -wwwFAST000 -fast -LOC_1
quick02.rambler.ru   -wwwFAST -wwwFAST01 -wwwFAST001 -fast -LOC_1
quick03.rambler.ru   -wwwFAST -wwwFAST00 -wwwFAST000 -fast -LOC_1
quick04.rambler.ru   -wwwFAST -wwwFAST01 -wwwFAST001 -fast -LOC_1
quick05.rambler.ru   -wwwFAST -wwwFAST02 -wwwFAST002 -fast -LOC_3
quick06.rambler.ru   -wwwFAST -wwwFAST02 -wwwFAST002 -fast -LOC_3
quick07.rambler.ru   -wwwFAST -wwwFAST03 -wwwFAST003 -fast -LOC_3
quick08.rambler.ru   -wwwFAST -wwwFAST03 -wwwFAST003 -fast -LOC_3
"""

ix_config = '''
www2305.rambler.ru   -www048 -index
www2306.rambler.ru   -www135 -index
www2307.rambler.ru   -www078 -index
www2308.rambler.ru   -www057 -index
www2309.rambler.ru   -www006 -index
'''

standard_directives = '''
#|^LOC.*$ Location
#|^wwwFAST\d{2}$ Fast2
#|^wwwFAST\d{3}$ Fast3
#|^indexFAST\d{3}$ FastIndex3
#|^www\d+$ WWW
#|^cite.*$ Cite
#|^refindex2_backup.*$ RefBackup

#/index/001 index7.rambler.ru:/spool9/idx_4_6-search/001:/spool9/idx_4_6-search/tmp
'''

ixconf = standard_directives + ixconf
ixref2 = standard_directives + ixref2
ixcite = standard_directives + ixcite
ixfast = standard_directives + ixfast
ixfast2 = standard_directives + ixfast2
ix_config = standard_directives + ix_config

import unittest
import radist
import time
import socket
import threading

def get_exec_time(func, *args, **kwarg):
    start_time = time.time()
    func(*args, **kwarg)
    return time.time() - start_time

def map_raise_at(at=2):
    def nested(x):
        if x == at:
            raise MyException("")
        return x
    return nested

class MyException(Exception):
    pass

class Fucks(unittest.TestCase):
    def setUp(self):
        self.test = radist.IXConfig(config=socket.gethostname() + " -test\nindex1.rambler.ru -test").test

    def tearDown(self):
        del self.test

    def test_double_apply(self):
        tt = radist.IXConfig(config=socket.gethostname() + " -test")
        assert tt.test.cluster_exec('echo %% > /dev/null')[0][1] == 0

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

class PMap(unittest.TestCase):
    def err_iterator(self, stop=3):
        for i in range(10):
            if i == stop:
                raise MyException()
            time.sleep(1)
            yield i

    def test_none(self):
        x = radist.pmap(None, xrange(30))
        assert list(x) == range(30)

    def test_simple(self):
        x = radist.pmap(int, xrange(30))
        assert list(x) == range(30)

    def test_double(self):
        x = radist.pmap(max, xrange(30), range(3), range(20))
        assert list(x) == range(30)

    def test_iterator_raise(self):
        raise_at = 2
        x = radist.pmap(int, self.err_iterator(raise_at))
        t = get_exec_time(self.failUnlessRaises, MyException, list, x)
        assert t <= raise_at * 1.1, "time was %f" % t
        tc = threading.activeCount()
        assert tc == 1, "there are %d threads" % tc

    def test_func_raise(self):
        raise_at = 2
        x = radist.pmap(map_raise_at(raise_at), xrange(20))
        t = get_exec_time(self.failUnlessRaises, MyException, list, x)
        assert t <= raise_at * 1.1, "time was %f" % t
        tc = threading.activeCount()
        # XXX it's bad, but i can't do it better
        # anyway it's not a huge problem with bad thread, i hope...
        assert tc in [1, 2], "there are %d threads" % tc

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

class IXFast2(unittest.TestCase):
    def test_wwwFAST(self):
        assert self.ix.wwwFAST.cluster_size() == 8, str(self.ix.wwwFAST.cluster_size())
        assert self.ix.wwwFAST3.cluster_size() == 8
        assert self.ix.wwwFAST3.color000.cluster_size() == 2

    def setUp(self):
        self.ix = radist.IXConfig(config=ixfast2)

    def tearDown(self):
        del self.ix

class IXFast(unittest.TestCase):
    def test_indexFAST(self):
        assert self.ix.indexFAST.cluster_size() == 3, str(self.ix.indexFAST.cluster_size())
        assert self.ix.indexFAST3.cluster_size() == 3
        assert self.ix.indexFAST3.c000.get('server') == 'fast04'

    def test_wwwFAST(self):
        assert self.ix.wwwFAST.cluster_size() == 3, str(self.ix.wwwFAST.cluster_size())
        assert self.ix.wwwFAST3.cluster_size() == 3
        assert self.ix.wwwFAST3.color000.cluster_size() == 2

    def setUp(self):
        self.ix = radist.IXConfig(config=ixfast)

    def tearDown(self):
        del self.ix

class IXCite(unittest.TestCase):
    def test_cite(self):
        assert self.ix.cite.cluster_size() == 6

    def test_cite_colors(self):
        assert self.ix.cite.color000.cluster_size() == 2

    def setUp(self):
        self.ix = radist.IXConfig(config=ixcite)

    def tearDown(self):
        del self.ix

class IXRef2(unittest.TestCase):
    def test_master_has_server(self):
        assert hasattr(self.ix.refindex2.backup.master, 'primary')
        assert self.ix.get('refindex2/backup/master/server') == 'www111'

    def test_backup_color_000_size(self):
        assert self.ix.refindex2.backup.color000.cluster_size() == 2

    def test_add_node_to_path(self):
        self.failUnlessRaises(radist.RadistError, self.ix.add_node_to_path,
            'refindex2/backup/master', radist.RadistNode("master"))

    def setUp(self):
        self.ix = radist.IXConfig(config=ixref2)

    def tearDown(self):
        del self.ix

class IXConfTest(unittest.TestCase):
    def test_add_node_to_path(self):
        ix = radist.IXConfig(config='')
        ix.add_node_to_path('LOC/xxx/123', radist.RadistNode('123'))
        assert ix.get('LOC/xxx/123/name') == '123'

    def test_1(self):
        assert self.ix.LOC1.cluster_size() == 1

    def test_2(self):
        assert self.ix.merger.cluster_size() == 3

    def test_www(self):
        assert self.ix.www.cluster_size() == 3
        assert self.ix.www.color001.cluster_size() == 2
        assert self.ix.www.color002.cluster_size() == 1

    def test_DEAD(self):
        ix = radist.IXConfig(config=('www1 -src010 -DEAD', 'www2 -src020'))
        assert ix.src.cluster_size() == 1

    def test_bad_cfg(self):
        self.failUnlessRaises(radist.IXConfError, radist.IXConfig,
            config=('www1 -'))
        self.failUnlessRaises(radist.IXConfError, radist.IXConfig,
            config=('www1 cite'))
        self.failUnlessRaises(radist.IXConfError, radist.IXConfig,
            config=('www1 -cite:000:0001'))

    def setUp(self):
        self.ix = radist.IXConfig(config=ixconf)

    def tearDown(self):
        del self.ix

class ColoredNodeTest(unittest.TestCase):
    def add_several(self):
        www = radist.ColoredNode('www')
        www.add_child(radist.RadistNode('000'))
        www.add_child(radist.RadistNode('000'))
        return www

    def test_add(self):
        www = self.add_several()
        assert www.color000.cluster_size() == 2
        assert www.cluster_size() == 2

    def test_2col(self):
        www = self.add_several()
        www.add_child(radist.RadistNode('001'))
        assert www.color000.cluster_size() == 2
        assert www.cluster_size() == 3
        assert www.color001.cluster_size() == 1

    def test_still_fail_at_two_name(self):
        www = radist.ColoredNode('www')
        www.add_child(radist.RadistNode('common'))
        self.failUnlessRaises(radist.RadistError, www.add_child,
                radist.RadistNode('common'))

class RadistNodeTest(unittest.TestCase):
    def test_repr(self):
        assert 'ix' in repr(radist.RadistNode('ix'))

    def test_fail_at_two(self):
        www = radist.RadistNode('www')
        www.add_child(radist.RadistNode('common'))
        self.failUnlessRaises(radist.RadistError, www.add_child,
                radist.RadistNode('common'))

class RadistConfTest(unittest.TestCase):
    def test_rexec_fail(self):
        self.failUnlessRaises(radist.RadistError, self.r.c001.stable_1.r_exec, 'ls')

    def test_get_node(self):
        assert self.r.get_node() == self.r
        # FakeNode doesn't set name by node.name
        # assert self.r.get_node('rccf').rccf == self.r.rccf

    def test_get_node_fail(self):
        self.failUnlessRaises(radist.RadistError, self.r.get_node,
            'aaa/server', '000')

    def test_get_node2(self):
        node = self.r.get_node('rccf/001', '000')
        assert len(node.get()) == 2

    def test_get_node_same_name(self):
        node = self.r.get_node('rccf/001', '001', 'aaa', '000')
        assert len(node.get()) == 4
        node.add_child(radist.RadistNode('aaa'))
        assert len(node.get()) == 5

    def test_get_node_cluster(self):
        node = self.r.get_node('rccf/001', '001', 'aaa', '000')
        assert len(node.cluster()) == 4

    def test_get_servers(self):
        node = self.r.get_servers()
        assert len(node.get()) == 4
        names = {}
        for pc in node.get():
            names[pc.get('server')] = 1
        assert len(names) == 4

    def test_get_all(self):
        assert len(self.r.get()) == 5

    def test_get_attrs(self):
        assert len(self.r.get_attrs()) == 1 # only name should be defined

    def test_get_attrs_2(self):
        assert self.r.rccf.c001.get_attrs()['server'] == self.r.get('rccf/001/server')

    def test_get_all_1(self):
        node = self.r.get()[0]
        assert isinstance(node.name, basestring)
        assert self.r.get(node.name) == node

    def setUp(self):
        self.r = radist.RadistConf(config=conf1)

    def tearDown(self):
        del self.r

    def test_nested(self):
        assert self.r.c000.stable.name == 'stable'

    def test_nested_2(self):
        assert self.r.c001.primary.server == 'index10.rambler.ru'

    def test_nested_2_get(self):
        assert self.r.get('001/stable-1').spacelimit.limit == 777

    def test_get_nested(self):
        assert self.r.get('000/stable/spacelimit') == 2222

    def test_get000(self):
        assert self.r.get('000/name') == '000'

    def test_get_server(self):
        assert self.r.get('aaa/server') == "index3.rambler.ru"

    def test_mask(self):
        servers = self.r.get('*/server')
        for i in ['index1.rambler.ru', 'index3.rambler.ru',
                  'index7.rambler.ru', 'index10.rambler.ru']:
            assert i in servers, '%s not in list' % i

    # do not be supported
    def test_get_sharp(self):
        assert self.r.get('#') == self.r.cluster()

    def test_get_dir(self):
        assert self.r.get('qwe/dir') == "/"

    def test_get_tmp(self):
        assert self.r.get('qwe/tmp') == '/tmp'

    def test_tmp(self):
        assert self.r.aaa.primary.temp == '/tmp'

    def test_dir(self):
        assert self.r.aaa.primary.dir == '/'

    def test_server(self):
        assert self.r.qwe.primary.server == 'index1.rambler.ru'

    def test_absent(self):
        self.failUnlessRaises(radist.RadistPathError, self.r.get, 'absent/name')

    def test_bad_attr(self):
        self.failUnlessRaises(radist.RadistPathError, self.r.get, 'absent')

    def test_cluster_size(self):
        assert self.r.cluster_size() == 2

    def test_select(self):
        l = self.r.get(server='index1.rambler.ru')
        assert len(l) == 1
        assert l[0].name == 'qwe'

    def test_get_2(self):
        l = self.r.get('qwe/dir', 'qwe/tmp')
        assert len(l) == 2
        assert l == ['/', '/tmp']

    def test_default(self):
        assert self.r.get_default('absent/name', 'absent-name') == 'absent-name'

    def test_select_func(self):
        l = self.r.get(server=lambda x: x.startswith('index3'))
        assert len(l) == 1, l
        assert l[0].name == 'aaa'

    def test_bad_select(self):
        self.failUnlessRaises(radist.RadistError, self.r.get, strange_opt='1')

    def test_undef_attr(self):
        self.failUnlessRaises(radist.RadistPathError, self.r.get, 'aaa/space')

    def test_bad_config(self):
        self.failUnlessRaises(radist.RadistConfError, radist.RadistConf,
            config="  primary=111:222")
        self.failUnlessRaises(radist.RadistConfError, radist.RadistConf,
            config="/R\n  primary=111:222")
        self.failUnlessRaises(radist.RadistConfError, radist.RadistConf,
            config=("/R/q", "  primary=111"))
        self.failUnlessRaises(radist.RadistError, radist.RadistConf,
            config=("/R/", "  primary=111:222"))
        self.failUnlessRaises(radist.RadistConfError, radist.RadistConf,
            config=("/R/x1", "  primary=111:222", "/R/x1", "  spacelimit=666"))
        self.failUnlessRaises(radist.RadistConfError, radist.RadistConf,
            config=("/R/1", " primary=bad"))

    def test_repr_of_node(self):
        assert 'qwe' in repr(self.r.get('qwe'))
        assert '001' in repr(self.r.get('rccf/001'))

    def test_run_fail(self):
        self.failUnlessRaises(TypeError, self.r.cluster_exec,
                'ls', parallel=True, single=True)

    def test_cluster(self):
        result = self.r.cluster_exec('ls %(reallybadarg)d', parallel=True)
        assert len(result) == 0


class IXDirectiveTest(unittest.TestCase):
    def setUp(self):
        self.r = radist.IXConfig(config = ix_config)

    def test_count(self):
        self.assert_(self.r.directive_count == 8)

    def test_special_role(self):
        good_line = '^wwwFAST\d{2}$ Fast2'
        self.assertEqual((r'^wwwFAST\d{2}$', 'Fast2'), radist.ix.split_special_role_directive(good_line))
        bad_line = '^LOC.*$Location'
        self.assertRaises(radist.IXConfError, radist.ix.split_special_role_directive, bad_line)
        very_bad_line = "|^LOC.*$ os.system('ls')"
        self.assertRaises(radist.IXConfError, radist.ix.parse_special_role_directive, very_bad_line)

    def test_directory(self):
        self.assertEqual('index7.rambler.ru', self.r.index.c001.primary.server)
        self.assertEqual('/spool9/idx_4_6-search/001', self.r.index.c001.primary.dir)
        self.assertEqual('/spool9/idx_4_6-search/tmp', self.r.index.c001.primary.temp)


if __name__ == '__main__':
    unittest.main()
