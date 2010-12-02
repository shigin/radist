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
    if 'RADIST_CONF_URL' in os.environ:
        url = os.environ['RADIST_CONF_URL']
    elif 'RADIST_CONF_PATH' in os.environ or 'RADIST_CONF_HOST' in os.environ:
        url = os.environ.get('RADIST_CONF_HOST', '') + \
              os.environ.get('RADIST_CONF_PATH', '/rambler/radist.conf')
    else:
        url = 'http://conf.rambler.ru/rambler/radist.conf'
        
    return try3(RadistConf, url)

def get_ix():
    """Returns default ixServers tree.

    It try to do it 3 times."""
    url = os.environ.get('IXSEARCH_SERVERMAP', 'http://conf.rambler.ru/rambler/ixServers.cfg')
    return try3(IXConfig, url)
