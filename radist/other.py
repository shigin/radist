import mux_fds
from collections import defaultdict

def get_stdouts(node, command, cluster):
    """Returns open stdouts."""
    stdouts = []
    for server in node.cluster(cluster):
        stdin, stdout = server.r_popen2(command)
        stdin.close()
        stdouts.append((stdout, server))
    return stdouts

# TODO: write case test
def cluster_read(node, command, cluster=True):
    """Returns mapping node -> command result.

    Command would execute on every cluster machine in node.
    """
    result = defaultdict(list)
    stdouts = get_stdouts(node, command, cluster)
    for node, line in mux_fds.AdvMuxReader(stdouts):
        result[node].append(line)
    return dict((node, '\n'.join(lines))
        for node, lines in result.iteritems())

# TODO: write case test
def cluster_open(node, command, cluster=True):
    """Returns iterator which yields pair (node, string).

    Command would execute on every machine in node.
    """
    stdouts = get_stdouts(node, command, cluster)
    return mux_fds.AdvMuxReader(stdouts)
