import mux_fds
from collections import defaultdict

# TODO: write case test
def cluster_read(node, command, cluster=True):
    """Returns mapping node -> command result.
    
    Command would execute on every cluster machine in node.
    """
    stdouts = []
    if cluster:
        nodes = node.cluster()
    else:
        nodes = node.get()
    for server in nodes:
        stdin, stdout = server.r_popen2(command % server.get_attrs())
        stdin.close()
        stdouts.append((stdout, server))

    result = defaultdict(list)
    for node, line in mux_fds.AdvMuxReader(stdouts):
        result[node].append(line)
    return dict((node, '\n'.join(lines))
        for node, lines in result.iteritems())
