import time
import radist
ix = radist.get_ix()

def test_run_1(self):
    stdout = []
    stderr = []
    start_time = time.time()
    print "start time: %s" % time.ctime(start_time)
    for server in ix.refindex2.get_servers(all_nodes=True).get():
        stds = server.r_popen3('hostname; sleep 2; echo all ok')
        stderr.append(stds[2])
        stdout.append(stds[1])

    mr = radist.MuxReader(stdout)
    print "MuxReader: %d sec" % int(time.time() - start_time)
    start_time = time.time()
    all_ok = False
    for line in mr:
        if line == "all ok" and not all_ok:
            print "first all ok: %d sec" % int(time.time() - start_time)
            assert int(time.time() - start_time) < 3
            all_ok = True

    assert all_ok, "can't find any 'all ok'"
    assert int(time.time() - start_time) < 5
    print "finish: %d sec" % int(time.time() - start_time)

def test_run_adv(self):
    stdout = []
    stderr = []
    start_time = time.time()
    print "start time: %s" % time.ctime(start_time)
    for server in ix.refindex2.get_servers(all_nodes=True).get():
        stds = server.r_popen3('hostname; sleep 2; echo all ok')
        stderr.append((stds[2], server.primary.server))
        stdout.append((stds[1], server.primary.server))

    mr = radist.AdvMuxReader(stdout)
    print "MuxReader: %d sec" % int(time.time() - start_time)
    start_time = time.time()
    all_ok = False
    for obj, line in mr:
        if line == "all ok" and not all_ok:
            print "first all ok: %d sec" % int(time.time() - start_time)
            assert int(time.time() - start_time) < 3
            all_ok = True
        if line.endswith('rambler.ru'):
            assert line == obj, "%s != %s" % (line, obj)

    assert int(time.time() - start_time) < 5
    print "finish: %d sec" % int(time.time() - start_time)

if __name__ == '__main__':
    test_run_1(None)
    test_run_adv(None)
