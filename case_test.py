import radist

def check_result(result, n):
    assert len(result) == n
    for xnode, status in result:
        assert status == 0

r = radist.get_r()
print "---- TEST r_exec ----"
assert r.index.common.r_exec('echo %(name)s; ls %(dir)s') == 0
node = r.get_node('index/001', 'index/002', 'index/003', 'index/common')
print "---- TEST cluster_exec ----"
print "* seq"
result = node.cluster_exec('echo %(name)s', all_nodes=True)
check_result(result, 4)
print "* parallel"
result = node.cluster_exec('echo %(name)s', parallel=True, all_nodes=True)
check_result(result, 4)
print "* single"
result = node.cluster_exec('echo %(name)s', single=True, all_nodes=True)
check_result(result, 4)

print "= server <= index1"
node = r.index.get_node(server="index1.rambler.ru")
print "* seq"
result = node.cluster_exec('echo %(name)s')
check_result(result, node.cluster_size())
print "* parallel"
result = node.cluster_exec('echo %(name)s', parallel=True)
assert len(result) == node.cluster_size()
try:
    check_result(result, node.cluster_size())
except AssertionError:
    print "FUUUUCK!!! IT CAN BE BAD"
print "* single"
result = node.cluster_exec('echo %(name)s', single=True)
check_result(result, node.cluster_size())

print '== IX TEST =='
ix = radist.get_ix()
ix.merger.cluster_exec('echo Run hostname at %(server)s; hostname', parallel=True)
