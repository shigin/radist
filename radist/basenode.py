import os
import socket
import fnmatch
import helpers
import r_pexec
import alive
from errors import *
from attrs import *

def shell_escape(string):
    """Escape string to safely use in shell scripts"""
    o = string.replace('\\', '\\\\').replace('$', '\\$').replace('"', '\\"')
    return '"' + o + '"'

def has_magic(name):
    """Returns if name has meta-characters. """
    import re
    return re.match(r'[?*[]', name)

def get_node(server, root_dir='/', tmp_dir='/tmp'):
    """Creates node with server, root and temp directory."""
    node = RadistNode(server)
    node.add_attr('primary', RadistPrimary('%s:%s:%s' % (server, root_dir, tmp_dir)))
    return node

def local_node(root_dir='/', tmp_dir="/tmp"):
    """Creates node for current host."""
    return get_node(socket.gethostname(), root_dir, tmp_dir)

def make_child_name(name):
    if name.isdigit():
        return 'c' + name
    cname = name
    if " " in name:
        cname = cname.replace(" ", "_")
    if "-" in name:
        cname = cname.replace("-", "_")
    return cname
        
def adv_extend(list, nodes):
    if isinstance(nodes, (RadistNode, basestring)):
        list.append(nodes)
    else:
        list.extend(nodes)

class RadistNode(object):
    """Represents node of the radist config."""
    def __init__(self, value):
        self.name = value
        self.__childs = {}
        self.has_attrs = False

    def __repr__(self):
        if getattr(self, 'primary', None) is not None:
            attr = " server: '%s'" % self.primary.server
        else:
            attr = ''
        if self.__childs:
            attr += ' childs: %d' % len(self.__childs)
        return "<%s '%s'%s>" % (self.__class__.__name__, self.name, attr)

    def put_file(self, out, file_name=None, lines=None, **kwargs):
        """Routine puts file to 'out' location on the remote machine.

        The source file can be specified by file_name or by any iterable
        in lines.

        Routine raises EnvironmentError if error occurred. 
        """
        devnull = open(os.devnull)
        if lines is None:
            lines = open(file_name)
        else:
            assert file_name is None
        if isinstance(lines, basestring):
            lines = (lines, )
        pid, stdin = self.r_exec(
            'cat > ' + shell_escape(out % self.get_attrs()),
            flags=helpers.R_SUB2, stdout=devnull, **kwargs)
        for line in lines:
            stdin.write(line)
        stdin.close()
        _, status = os.waitpid(pid, 0)
        if os.WIFEXITED(status):
            exit_code = os.WEXITSTATUS(status)
            if exit_code != 0:
                raise EnvironmentError(
                    "Child returned with non-zero exit code", exit_code)
        else:
            raise EnvironmentError("Child returned with status", status)

    def cluster_put(self, out, file_name=None, lines=None, **kwargs):
        """Put file on a remote machines.
        
        Please be careful, file would be read into the memory, if lines 
        isn't tuple, string or list it would be copied!

        For details please look at put_file."""
        if file_name:
            assert lines is None
            lines = list(open(file_name))
        if not isinstance(lines, (list, tuple)):
            lines = tuple(lines)
        for machine in self.cluster():
            machine.put_file(out, lines=lines, **kwargs)

    def cluster(self):
        """Returns cluster nodes."""
        return [self.__childs[key] for key in self.__childs.keys() \
                if key.isdigit()]

    def cluster_size(self):
        """Returns cluster size of node."""

        return len(self.cluster())

    def add_attr(self, name, attr):
        """Adds attribute to node."""
        setattr(self, name, attr)
        self.has_attrs = True

    def get_attrs(self):
        """Returns dictionary with attributes of node."""
        info = {}
        for path in find_map:
            attr = map_get_unsafe(self, path)
            if attr != None:
                info[path] = attr
        return info

    def __get_exec_list(self, cmd, all_nodes):
        result = {}
        if all_nodes:
            nodes = self.__childs.values()
        else:
            nodes = self.cluster()
        for child in nodes:
            if hasattr(child, 'primary'):
                server = child.primary.server
                try:
                    command = cmd % child.get_attrs()
                    result[child] = command
                except KeyError:
                    # can't create command, skip server
                    continue
        return result

    def __wait_parallels(self, pids):
        result = []
        for _pid, node in pids.items():
            pid, status = os.wait()
            if pid == -1:
                # XXX
                pass
            if pid not in pids:
                # XXX it's REALLY strange, we should inform user, but 
                # i don't know how
                pass
            else:
                result.append((pids[pid], status))
        return result

    def __run_single(self, queue, **kwargs):
        result = []
        pids = {}
        for server in queue.keys():
            node, command = queue[server].pop()
            pid = helpers.r_exec(server, command, flags=os.P_NOWAIT, **kwargs)
            pids[pid] = node

        while len(pids) > 0:
            pid, status = os.wait()
            if pid in pids:
                node = pids[pid]
                del pids[pid]
                result.append((node, status))
                if len(queue[node.primary.server]) > 0:
                    node, command = queue[node.primary.server].pop()
                    server = node.primary.server
                    pid = helpers.r_exec(server, command, flags=os.P_NOWAIT, **kwargs)
                    pids[pid] = node
            else:
                # XXX shit happens
                pass
        return result
        
    def cluster_exec(self, cmd, single=False, parallel=False, 
                all_nodes=False, check=False, timeout=None, **kwargs):
        """Runs cmd on cluster.
        
        If single is True, commands will run parallel, but only one 
        command on server.

        If parallel is True, commands will run parallel and wouldn't
        see how many ones on one server.
        
        If both is false, one command per time.
        
        If all_nodes is true, runs command on all child of the node,
        else only on cluster (e.g. nodes with numeric name).
        
        If check is true, runs command only if all servers in node is alive.
        Optional argument is 'timeout', it's a timeout to determine if server 
        is alive. See get_dead_nodes for detail.
        """
        if single and parallel:
            raise TypeError("single OR parallel, not both")
        if (timeout is not None) and not check:
            raise TypeError("timeout hasn't got any sense without check=True")
        if check:
            if not alive.is_alive(self, all_nodes=all_nodes, **kwargs):
                raise RadistError('some nodes is dead')
        devnull = file(os.devnull, 'r')
        result = []
        pids = {}
        queue = {}
        to_run = self.__get_exec_list(cmd, all_nodes)
        for node, command in to_run.items(): 
            server = node.primary.server
            if parallel:
                pid = helpers.r_exec(server, command, 
                    flags=os.P_NOWAIT, stdin=devnull, **kwargs)
                pids[pid] = node
            elif single:
                if server not in queue:
                    queue[server] = []
                queue[server].append((node, command))
            else:
                result.append((node, helpers.r_exec(server, command, **kwargs)))
        if parallel:
            return self.__wait_parallels(pids)
        elif single:
            return self.__run_single(queue, **kwargs)
        else:
            return result
    
    def _get_server(self):
        if hasattr(self, 'primary'):
            return self.primary.server
        else:
            raise RadistError("can't run command on node without primary server.")
        
    def r_exec(self, command, **kwargs):
        """Run command on node."""
        return helpers.r_exec(self._get_server(), 
                    command % self.get_attrs(), **kwargs)

    def r_popen2(self, command, **kwargs):
        """Run command on node."""
        return helpers.r_popen2(self._get_server(), 
                    command % self.get_attrs(), **kwargs)

    def r_popen3(self, command, **kwargs):
        """Run command on node."""
        return helpers.r_popen3(self._get_server(),
                command % self.get_attrs(), **kwargs)

    def r_pexec(self, func, *args, **kwargs):
        """Run python function on selected node."""
        return r_pexec.r_pexec(self._get_server(), func, *args, **kwargs)

    def r_executor(self, func, user):
        """Returns function what will be executed on node.
        
        If you do not want run function from different user it's better
        to user r_pexec.

        WARNING: you can call function only once!!!"""
        return r_pexec.executor(self._get_server(), func, user)

    def _select(self, **kwargs):
        """Returns list with node, which attributes equals to given values.
        
        If given value is callable, value calls with attribute as argument.
        
        Sample:
        In [64]: r.get(server="index1.rambler.ru")
        Out[64]: 
        [<RadistNode 'top100-refindex' server: 'index1.rambler.ru'>,
         <RadistNode 'build_fhsearch' server: 'index1.rambler.ru'>]

        In [65]: r.get(server=lambda x: x.startswith("index1"))
        Out[65]: 
        [<RadistNode 'top100-refindex' server: 'index1.rambler.ru'>,
         <RadistNode 'build_fhsearch' server: 'index1.rambler.ru'>]
        """
        for i in kwargs.keys(): 
            if i not in find_map.keys():
                raise RadistPathError('Invalid attribute path', i)

        result = self.__childs.values()
        for i in kwargs.keys():
            if not callable(kwargs[i]):
                func = lambda x: kwargs[i] == x
            else:
                func = kwargs[i]

            temp_result = []
            for node in result:
                try:
                    attr = map_get(node, i)
                    if func(attr):
                        temp_result.append(node)
                except RadistPathError:
                    # attribute for node isn't defined, it's ok
                    pass
            result = temp_result

        return result

    def iter_childs(self):
        """Iterate over children.
        
        Generate yields pair (name, node).
        """
        for name, node in self.__childs.iteritems():
            yield name, node
            for child_name, child_node in node.iter_childs():
                yield '/'.join([name, child_name]), child_node
        for pair in self.get_attrs().items():
            yield pair

    def __singe_get(self, name, mask):
        result = []
        if name == "#":
            return self.cluster()
        elif name in self.__childs:
            return [self.__childs[name]]
        elif name in find_map:
            if mask:
                if map_get_unsafe(self, name) is None:
                    return []
                else:
                    return [map_get(self, name)]
            else:
                return [map_get(self, name)]
        elif has_magic(name):
            result = []
            for cname, cnode in self.__childs.items():
                if fnmatch.fnmatchcase(cname, name):
                    result.append(cnode)
            if result or mask:
                return result
            else:
                raise RadistPathError("Bad child path.", name)                           
        else:
            raise RadistPathError("Bad child path.", name)

    def __rest_get(self, splited, mask):
        node_name, rest = splited
        if node_name in self.__childs:
            return [self.__childs[node_name]._attr_get([rest], mask)]
        elif node_name == '#':
            result = []
            for node in self.cluster():
                adv_extend(result, node._attr_get([rest], True))
            return result
        elif has_magic(node_name):
            result = []
            for cname, node in self.__childs.items():
                if fnmatch.fnmatchcase(cname, node_name):
                    adv_extend(result, node._attr_get([rest], True))
            if result or mask:
                return result
            else:
                raise RadistPathError("Bad child path.", node_name)
        else:
            raise RadistPathError("Bad child path.", node_name)

    def _attr_get(self, args, mask=False):
        """Returns node or attribute with path.
        
        Sample example:
        In [57]: r.get('rccf/001/server', 'rccf/001')
        Out[57]: ['webbase05.rambler.ru', <RadistNode '001' server: 'webbase05.rambler.ru'>]

        In [58]: r.get('index/001/server')
        Out[58]: 'index7.rambler.ru'

        In [59]: r.get('index/notexist') # return None

        """
        result = []
        for arg in args:
            splited = arg.split('/', 1)
            if len(splited) == 1:
                result.extend(self.__singe_get(splited[0], mask))
            else:
                result.extend(self.__rest_get(splited, mask))

        if len(result) == 1 and len(args) == 1:
            return result[0]
        else:
            return result

    def get(self, *args, **kwargs):
        """Returns attributes or nodes.
        
        *arg represent path to node/attribute.
        **kwargs selects all nodes with arguments.

        See _attr_get and _select for details.

        If you want to add attribute, you need to edit find_map.
        """
        if len(args) > 0:
            return self._attr_get(args)
        elif len(kwargs) > 0:
            return self._select(**kwargs)
        else:
            return self.__childs.values()

    def get_node(self, *args, **kwargs):
        """Returns node with children from get(...).
        
        It won't create node if *arg and **kwarg is empty.
        """
        if len(args) + len(kwargs) == 0:
            return self
        else:
            nodes = self.get(*args, **kwargs)
            new = FakeNode(self.name+':new')
            if isinstance(nodes, RadistNode):
                new.add_child(nodes)
            else:
                for node in nodes:
                    if isinstance(node, RadistNode):
                        new.add_child(node)
                    else:
                        raise RadistError("can't create node if child isn't node")
            return new

    def get_default(self, arg, default=None):
        """Get node or attribute.
        
        If it's absent, return default value."""
        try:
            return self.get(arg)
        except RadistPathError:
            return default

    def get_servers(self, all_nodes=False):
        """Returns FakeNode with all machines from current node.
        
        If all_nodes is False [default], gets servers only from cluster."""
        servers = {}
        if all_nodes:
            from_nodes = self.__childs.values()
        else:
            from_nodes = self.cluster()
        for node in self.__childs.values():
            if hasattr(node, 'primary'):
                servers[node.primary.server] = True

        nodes = FakeNode('%s:servers' % self.name)
        for server in servers.keys():
            node = RadistNode(server.split('.')[0])
            node.add_attr('primary', RadistPrimary('%s:/:/tmp' % server))
            nodes.add_child(node)
        return nodes

    def add_child(self, node, name=None):
        """Adds child to node.
        
        Name is optional parameter. If it sets, it override node name."""
        assert isinstance(node, RadistNode)
        name = name or node.name
        if self.__childs.has_key(name):
            raise RadistError("duplicated %s/%s" % (self.name, name))
        if "" == name.strip():
            raise RadistError("bad node name")
        self.__childs[name] = node
        setattr(self, make_child_name(name), node)
        return name

import warnings
class FakeNode(RadistNode):
    def __init__(self, name):
        RadistNode.__init__(self, name)
        self.__latest = 0

    def __repr__(self):
        return "<FakeNode '%s' cluster: %d>" % \
                (self.name, self.cluster_size())

    def add_child(self, node, name=None):
        if name is not None:
            warnings.warn("ignore argument 'name' in FakeNode.add_child")
        self.__latest += 1
        return RadistNode.add_child(self, node, "%04d" % self.__latest)

    def cluster_exec(self, *args, **kwargs):
        if kwargs.has_key('all_nodes'):
            warnings.warn("ignore argument 'all_nodes' in FakeNode.cluster_exec")
        return RadistNode.cluster_exec(self, *args, **kwargs)
