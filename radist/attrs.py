import errors

find_map = {'server': 'primary.server',
            'dir':    'primary.dir',
            'space':  'spacelimit.limit',
            'spacelimit': 'spacelimit.limit',
            'name':   'name',
            'tmp':    'primary.temp',
           }


def map_get_unsafe(_node, _path):
    """Returns attribute of node, specified by _path."""
    node = _node
    path = find_map[_path]
    for i in path.split('.'):
        node = getattr(node, i, None)
        if node is None:
            return None
    return node

def map_get(node, path):
    """Returns attribute of node, specified by path."""
    if path not in find_map:
        raise errors.RadistPathError("Invalid attribute specification", path)
    attr = map_get_unsafe(node, path)
    if attr is not None:
        return attr
    else:
        raise errors.RadistPathError("Attribute isn't defined", path)

class RadistSpaceLimit(object):
    """Represents spacelimit attribute of the radist node.

    Attributes:
     * limit"""
    def __init__(self, value):
        self.limit = int(value.strip())

class RadistBackup(object):
    """Represents backup attribute of the radist node.

    Attributs:
     * raw"""
    def __init__(self, value):
        self.raw = value.strip()

class RadistPrimary(object):
    """Represent primary attribute of the radist node.

    Attributs:
     * server
     * dir,
     * [temp]
    """
    def __init__(self, value):
        triplet = value.split(':')
        assert len(triplet) in [2, 3]
        self.server, self.dir = [x.strip() for x in triplet[0:2]]
        if len(triplet) == 3:
            self.temp = triplet[2]
        else:
            self.temp = None

attr_map = {'primary': RadistPrimary,
            'backup': RadistBackup,
            'spacelimit': RadistSpaceLimit,
           }

def get_radist_value(line):
    """Returns instance of class, which represents value of radist node."""
    assert line.startswith('  ')
    key, value = line.split('=')
    key = key.strip()
    return key, attr_map[key](value)
