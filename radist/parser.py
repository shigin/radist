from content import adv_get_content, get_line_iterator
from basenode import RadistNode, make_child_name
from errors import *
from attrs import get_radist_value

class RadistConf(RadistNode):
    """Represents radist.conf file.
    
    Simple usage:
    
    In [43]: rc = RadistConf('http://conf.rambler.ru/rambler/radist.conf')

    In [44]: rc.rccf.c000.primary.server
    Out[44]: 'webbase09.rambler.ru'

    In [45]: rc.get('rccf/000/server')
    Out[45]: ['webbase09.rambler.ru']

    In [46]: rc.rccf.get(name="000")
    Out[46]: [<RadistNode '000' server: 'webbase09.rambler.ru'>]
    """

    def __init__(self, URI=None, config=None):
        """URI is the link to config.
        
        config is string represent of config.
        
        For tests you can use something like
        In [6]: r = radist.RadistConf(config='''/R/qwe
          primary=index1.rambler.ru:/:/tmp'''
        )

        """
        RadistNode.__init__(self, 'R')
        content = adv_get_content(URI, config)
        node = None
        for line in get_line_iterator(content):
            if line.startswith('/R'):
                path = line.split('/')[2:]
                if len(path) > 0:
                    node = self
                    while len(path) > 0:
                        xdir = path.pop(0)
                        nnode = getattr(node, make_child_name(xdir), None)
                        if nnode == None:
                            nnode = RadistNode(xdir)
                            try:
                                node.add_child(nnode)
                            except RadistError, err:
                                raise RadistConfError(str(err))
                        elif len(path) == 0 and nnode.has_attrs:
                            raise RadistConfError('dublicated path: %s' % line)
                        node = nnode
                else:
                    raise RadistConfError("don't know how works with R-path /R")
            elif line.startswith('  '):
                if node is None:
                    raise RadistConfError('value before /R/ (%s)' % line)
                else:
                    try:
                        key, value = get_radist_value(line)
                        node.add_attr(key, value)
                    except AssertionError, err:
                        raise RadistConfError(str(err))
            elif line.strip() == '':
                pass
            else:
                raise RadistConfError("can't parse string %s" % line)
