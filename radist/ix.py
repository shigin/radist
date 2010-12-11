"""Presents ixServers.cfg like RadistNode. ixServers.cfg must be with special role directives."""
import re
from attrs import RadistPrimary
from basenode import RadistNode, FakeNode
from extnode import ColoredNode
from errors import *
from content import adv_get_content, get_line_iterator

special_role_mark = '|'

# Directives for special roles are started with "#|"
# #|role_regex class_name
# Regex must match the whole line (i.e ^some_regex$)

# Add this to the beginning of ixServers.cfg:
#|^LOC.*$ Location
#|^wwwFAST\d{2}$ Fast2
#|^wwwFAST\d{3}$ Fast3
#|^indexFAST\d{3}$ FastIndex3
#|^www\d+$ WWW
#|^cite.*$ Cite
#|^refindex2_backup.*$ RefBackup

directory_mark = '/'
directives = ['%', '!', special_role_mark, directory_mark]

class Special(object):
    def __init__(self, role, attrs):
        self.role = role
        self.name = role.rstrip('0123456789').rstrip('_').replace('_', '/')
        self.node_name = self.name.split('/')[-1]

    def get_roles(self):
        name = self.role.rstrip('0123456789')
        if len(name) == len(self.role):
           return [(self.name, -1)]
        else:
           return [(self.name, int(self.role[len(name):]))]

    def get_extra_nodes(self):
        return [(self.name, RadistNode(self.node_name))]

class Fixed(Special):
    def __init__(self, role, attrs):
        Special.__init__(self, role + '_', attrs)

class Location(Special):
    def __init__(self, role, attrs):
        assert len(attrs) == 0
        Special.__init__(self, role.replace('_', '') + '_', attrs)

class WWW(Special):
    def get_extra_nodes(self):
        return [(self.name, ColoredNode(self.node_name))]

class Cite(WWW):
    def __init__(self, role, attrs):
        Special.__init__(self, role, attrs)
        self.attrs = attrs

    def get_roles(self):
        result = []
        for attr in self.attrs:
            result.append([self.name, int(attr)])
        return result

class RefBackup(Special):
    def __init__(self, role, attrs):
        Special.__init__(self, role, attrs)
        self.ripath = '/'.join(self.name.split('/')[0:2])

    def get_extra_nodes(self):
        return [(self.ripath, ColoredNode(self.node_name))]

    def get_roles(self):
        if self.name.endswith('master'):
            return [(self.ripath, 'master')]
        else:
            return Special.get_roles(self)

class Fast2(WWW):
    def __init__(self, role, attrs):
        WWW.__init__(self, role.replace('wwwFAST', 'wwwFAST2_'), attrs)

class Fast3(WWW):
    def __init__(self, role, attrs):
        WWW.__init__(self, role.replace('wwwFAST', 'wwwFAST3_'), attrs)

class FastIndex3(Special):
    def __init__(self, role, attrs):
        Special.__init__(self, role.replace('indexFAST', 'indexFAST3_'), attrs)


import types
def class_by_name(name):
    if not (name in globals() and type(globals()[name]) == types.TypeType):
        raise IXConfError, "Can't find class named %s" % name
    return globals()[name]


def split_special_role_directive(cleaned_line):
    'Split special role directive. cleaned_line must be without directive mark'
    space_pos = cleaned_line.rfind(' ')
    if space_pos == -1:
        raise IXConfError, "No space in directive: %s" % cleaned_line
    return cleaned_line[:space_pos], cleaned_line[space_pos + 1:]


def parse_special_role_directive(line):
    'Return tuple (regex, class). Regex and class name in line must be separated by space.'
    cleaned = line[1:]
    r, class_name = split_special_role_directive(cleaned)
    return (re.compile(r), class_by_name(class_name))


def is_special_role(special):
    'Return True if special is directive for special role'
    return special and special[0] == special_role_mark

def is_directory(special):
    'Return True if special is directive for directory'
    return special and special[0] == directory_mark


class IXConfig(RadistNode):
    def __init__(self, URI=None, config=None):
        self.clear_special_roles()
        self.directive_count = 0
        self.directories = {}

        RadistNode.__init__(self, 'ix')
        content = adv_get_content(URI, config)

        clusters = {}
        for line, special in get_line_iterator(content, directives):
            if is_special_role(special):
                self.directive_count += 1
                self.add_special_role(parse_special_role_directive(special))
            elif is_directory(special):
                self.update_directories(special)
                self.directive_count += 1

            x = self.__get_roles(line)
            if x is None:
                # server without roles, skip one
                continue
            server, roles = x
#            print "start dancing..."
            for role, attrs in roles:
                special = self.get_special(role, attrs)
                for path, node in special.get_extra_nodes():
                    if self.get_default(path, None) is None:
                        self.add_node_to_path(path, node)

                for path, number in special.get_roles():
                    if type(number) != int:
                        name = number
                    else:
                        if number == -1:
                            if path not in clusters:
                                clusters[path] = -1
                            clusters[path] += 1
                            number = clusters[path]
                        name = "%03d" % number
                    node = RadistNode(name)
                    if self.directories_for(path, name):
                        node.add_attr('primary', RadistPrimary(self.directories_for(path, name)))
                    else:
                        node.add_attr('primary', \
                            RadistPrimary('%(server)s:/spool/_R_/%(path)s:/spool/_R_/%(path)s/tmp-radist' % \
                                {'server': server, 'path': path.split('/')[-1]}))
                    self.add_node_to_path('/'.join([path, node.name]), node)


    def clear_special_roles(self):
        self.special_roles = []


    def get_special(self, role, attrs):
        for regex, special in self.special_roles:
            if regex.match(role):
                return special(role, attrs)
        return Special(role, attrs)


    def add_special_role(self, parsed):
        self.special_roles.append(parsed)


    def update_directories(self, special):
        'Update self.directories with data from special (which must be directory directive)'
        cleaned = special[1:]
        space_pos = cleaned.find(' ')
        name = cleaned[:space_pos]
        self.directories[name] = cleaned[space_pos + 1:]


    def directories_for(self, path, name):
        'Return directories or None if '
        key = '/'.join([path, name])
        return self.directories.get(key)


    def add_node_to_path(self, path, node):
        xnode = self
        fpath = path.split('/')
        while len(fpath) > 0:
            elem = fpath.pop(0)
            if xnode.get_default(elem, None) is None:
                if len(fpath) != 0:
                    xnode.add_child(RadistNode(elem))
                else:
                    xnode.add_child(node, elem)
            elif len(fpath) == 0:
                raise RadistError("dublicated path: %s" % path)
            if len(fpath) != 0:
                xnode = xnode.get(elem)

    def __get_roles(self, line):
        splited = line.split()
        if len(splited) in [0, 1]:
            # empty string or server without roles, skip it
            return None
        server = splited[0]
        raw_roles = []
        for role in splited[1:]:
            if not role.startswith('-'):
                raise IXConfError("role '%s' doesn't starts with -" % role)
            if len(role) == 1:
                raise IXConfError("can't parse role '%s'"% role)
            raw_roles.append(role[1:])
        if 'DEAD' in raw_roles:
            # skip dead servers
            raw_roles = ['DEAD']

        roles = []
        for raw in raw_roles:
            xraw = raw.split(':')
            role = xraw[0]
            if len(xraw) == 1:
                values = []
            elif len(xraw) == 2:
                values = xraw[1].split('+')
            else:
                raise IXConfError("can't parse role '%s'" % raw)
            roles.append([role, values])

        return (server, roles)
