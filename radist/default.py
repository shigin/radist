import os
from parser import RadistConf
from ix import IXConfig

def try3(func, url):
    import urllib2
    for i in range(3):
        try:
            return func(url)
        except urllib2.URLError, err:
            pass
    raise err

def get_r():
    """Returns default /R/ tree.

    It try to do it 3 times."""
    if get_r.r is None:
        if 'RADIST_CONF_URL' in os.environ:
            url = os.environ['RADIST_CONF_URL']
        elif 'RADIST_CONF_PATH' in os.environ or 'RADIST_CONF_HOST' in os.environ:
            url = os.environ.get('RADIST_CONF_HOST', '') + \
                  os.environ.get('RADIST_CONF_PATH', '/rambler/radist.conf')
        else:
            url = 'http://conf.rambler.ru/rambler/radist.conf'

        get_r.r = try3(RadistConf, url)
    return get_r.r
get_r.r = None

def set_r(location):
    """Set /R/ tree instead of default.

    The routine useful for overwrite default behaviour in scripts."""
    get_r.r = try3(RadistConf, location)

def get_ix():
    """Returns default ixServers tree.

    It try to do it 3 times."""
    if get_ix.ix is None:
        url = os.environ.get('IXSEARCH_SERVERMAP',
            'http://conf.rambler.ru/rambler/ixServers.cfg')
        get_ix.ix = try3(IXConfig, url)
    return get_ix.ix
get_ix.ix = None

def set_ix(location):
    """Set ixServers tree instead of default.

    The routine useful for overwrite default behaviour in scripts."""
    get_ix.ix = try3(IXConfig, location)
