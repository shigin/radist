"""This module contains method to handle varios URI scheme."""
import os, sys
import re
import itertools
import urllib2
import ftplib
import socket
import netrc
from warnings import warn
from radist.helpers import R_SUB2
scheme = re.compile('(?P<scheme>[a-z]+)://(?P<host>[a-z:0-9.]+)/(?P<path>.*)')
localhost = socket.gethostbyname(socket.gethostname())

def auth_from_wget(filename='~/.wgetrc'):
    DUSER = 'anonymous'
    DPASS = 'radist@rambler'
    try:
        wgetrc = open(os.path.expanduser(filename), 'r')
    except IOError:
        return DUSER, DPASS
    d = {}
    for line in wgetrc:
        try:
            key, val = line.split('#')[0].strip().split('=')
            d[key] = val
        except:
            # who matter
            pass
    return d.get('user', DUSER), d.get('passwd', DPASS)

def get_auth(host):
    info = get_auth.netrc.authenticators(host) 
    if info:
        host, account, passwd = info
        return host, passwd
    else:
        return get_auth.default

class FakeNetRC:
    def auth_from_wget(self, host):
        return None

try:
    get_auth.netrc = netrc.netrc()
except IOError:
    get_auth.netrc = FakeNetRC()
get_auth.default = auth_from_wget()

try:
    from functools import partial
except ImportError:
    def decorator_helper(func, wrapper):
        "Sets doc, module, name and dict of wrapper from func."
        wrapper.__doc__ = func.__doc__
        wrapper.__module__ = func.__module__
        try:
            # builtin functions hasn't got __dict__
            wrapper.__dict__.update(func.__dict__)
        except: 
            pass
        try:
            # xml rpc function hasn't got __name__
            wrapper.__name__ = func.__name__
        except:
            pass

    def partial(func, *arg, **kwargs):
        def nested(*args, **kwargs):
            if kwargs or ckwargs:
                dict = ckwargs.copy()
                dict.update(kwargs)
            else:
                dict = ckwargs or kwargs
            return func(*(cargs + args), **dict)

        decorator_helper(func, nested)
        return nested

class FileSocketProxy(object):
    def __init__(self, socket):
        self.__socket = socket

    def __getattr__(self, prop):
        attr = getattr(self.__socket, prop)
        return attr

    def write(self, str):
        return self.__socket.send(str)

__all__ = ['get_file', 'adv_get_content', 'get_line_iterator', 'file_fabric']

class Content(object):
    """Abstract class for get_class fabric.
    
    Childs of the class should returns True if can handle name.
    Method get_content returns object with readlines and read methods.
    """
    @staticmethod
    def is_me(name):
        """Returns True if can handle name."""
        return False

    @staticmethod
    def can_target(name):
        """Returns True if can create 'name' file"""
        return False

    @staticmethod
    def get_host(name):
        """Returns a name of the host for 'name'."""
        raise NotImplementedError('derived class should overload me')

    @staticmethod
    def get_file_write(name):
        "Returns file--like object, which can write to file."
        raise NotImplementedError('derived class should overload me')

    @staticmethod
    def get_content(name):
        """Returns object with readlines and read methods.
        
        Usually it's 'file' object."""
        raise NotImplementedError('derived class should overload me')

    @staticmethod
    def remote_get(name, src):
        """Copy file from src to remote target name."""
        raise NotImplementedError('derived class should overload me')

class StdIn(Content):
    "Represents stdin."
    @staticmethod
    def is_me(name):
        return name in (None, '', '-')

    @staticmethod
    def get_content(name):
        return sys.stdin

    @staticmethod
    def get_host(name):
        return socket.gethostbyname(socket.gethostname())

class LocalFile(Content):
    "Represents local file."
    @staticmethod
    def is_me(name):
        return name.startswith('/') or \
            name.startswith('file://') or \
            name.startswith('~') or \
            os.path.isfile(name)
    
    @staticmethod
    def can_target(name):
        return True

    @staticmethod
    def get_file_write(name):
        "Returns file--like object, which can write to file."
        return open(name, 'w')

    @staticmethod
    def get_host(name):
        return socket.gethostbyname(socket.gethostname())

    @staticmethod
    def get_content(name):
        return open(os.path.expanduser(name), 'r')

class URLFile(Content):
    "Files which can be accessed with urllib"
    cache = {}
    @staticmethod
    def is_me(name):
        return name.startswith('http://') or \
            name.startswith('ftp://') or \
            name.startswith('https://')
    
    @staticmethod
    def can_target(name):
        return name.startswith('ftp://')

    @staticmethod
    def get_host(name):
        parsed = scheme.match(name)
        assert parsed
        return parsed.groupdict()['host']

    @staticmethod
    def get_content(name):
        return urllib2.urlopen(name)

    @staticmethod
    def get_path(name):
        x = scheme.match(name)
        if x:
            return x.groupdict()['path']
        else:
            raise ValueError("can't match URI '%s'" % name)

    @staticmethod
    def get_file_write(name):
        assert URLFile.can_target(name)
        host = URLFile.get_host(name)
        user, passwd = get_auth(host)
        ftp = URLFile.cache.get('host', ftplib.FTP(host, user, passwd))
        ftp.voidcmd('TYPE I')
        conn = ftp.transfercmd('STOR ' + URLFile.get_path(name))
        # ftp.voidresp()
        return FileSocketProxy(conn)

class SVNFile(URLFile):
    "Represents SVN file. It silly and should be removed!"
    @staticmethod
    def is_me(name):
        return name.startswith('svn://')

    @staticmethod
    def get_content(name):
        return urllib2.urlopen(name.replace('svn://', 'http://'))

class RadistFile(Content):
    "Represent radist location"
    @staticmethod
    def is_me(name):
        return name.startswith('ra://')

    @staticmethod
    def can_target(name):
        return True

    @staticmethod
    def get_host(name):
        node, path = RadistFile.get_tuple(name)
        return socket.gethostbyname(node.get('server'))

    @staticmethod
    def get_scheme(scheme):
        import default
        schemes = {
                    'ix': default.get_ix,
                    'r':  default.get_r,
                  }
        if scheme not in schemes:
            raise ValueError("don't know path '%s'" % scheme)
        if not hasattr(RadistFile, '__' + scheme):
            setattr(RadistFile, '__' + scheme, schemes[scheme]())
        return getattr(RadistFile, '__' + scheme)

    @staticmethod
    def get_tuple(name):
        "Returns tuple (RadistNode, path)"
        sch, xname = name.split('://', 1)
        assert sch == 'ra'
        server, path = xname.split('/', 1)
        if ':' in server:
            raise TypeError("can't work with parameters")
        scheme, rest = server.split('.', 1)
        radist = RadistFile.get_scheme(scheme)
        node = radist.get(rest.replace('.', '/'))
        if not path.startswith('/'):
            path = '%(dir)s/' + path
        return node, path

    @staticmethod
    def get_content(name):
        node, path = RadistFile.get_tuple(name)
        command = "cat '%s'" % path.encode('string_escape')
        stdin, stdout = node.r_popen2(command)
        stdin.close()
        return stdout

    @staticmethod
    def get_file_write(name):
        "Returns file--like object, which can write to file."
        node, path = RadistFile.get_tuple(name)
        if RadistFile.get_host(name) == localhost:
            return open(path, 'w')
        else:
            pid, stdin = node.r_exec("cat > " + path, flags=R_SUB2, stdout=sys.stdout)
            return stdin

class FileFabric(object):
    """Class is an ease access to *File class."""
    class Helper(object):
        def __init__(self, class_, name):
            self.__class = class_
            self.__name = name

        def __getattr__(self, prop):
            attr = getattr(self.__class, prop)
            if callable(attr):
                return partial(attr, self.__name)
            else:
                return attr

    def __init__(self):
        self.__readers = []
        module = sys.modules[__name__]
        for class_ in dir(module):
            content_obj = getattr(module, class_)
            if type(content_obj) == type and issubclass(content_obj, Content):
                self.__readers.append(content_obj)

    def __call__(self, URI):
        return self.get_wrapper(URI)

    def add_reader(self, helper):
        "Adds reader to query spool."
        self.__readers.append(helper)

    def get_class(self, URI):
        "Returns class which can handle URI."
        for reader in self.__readers:
            if reader.is_me(URI):
                return reader
        raise ValueError('Unsupported URI')

    def get_wrapper(self, URI):
        "Returns a instance of the class with binded URI."
        return FileFabric.Helper(self.get_class(URI), URI)

file_fabric = FileFabric()

def get_file(URI):
    """Opens URI and returns file object."""
    return file_fabric.get_class(URI).get_content(URI)

def adv_get_content(URI=None, config=None):
    """Returns content of URI or splited by \\n config.
    
    Example:
        def parse(uri=None, config=None):
            content = adv_get_content(uri, config)

    """
    if URI != None:
        content = get_file(URI)
    elif config != None:
        if isinstance(config, basestring):
            content = config.split('\n')
        elif hasattr(config, '__iter__'): # iterable
            content = config
        else:
            raise exception("can't hadle config, it must be string or iterable object")
    else:
        raise exceptions.TypeError('config or URI must be specified')
    return content

def get_line_iterator(iterable, special=[]):
    """Returns iterator over iterable.

    Iterator returns lines without trailing '\\n' and without 
    '#' style comments.
    """
    def helper(str):
        "Helper str -> str"
        pair = str.rstrip('\n').split('#', 1)
        if special: 
            if len(pair) == 1:
                return pair[0], None
            else:
                for i in special:
                    if pair[1].startswith(i):
                        return pair
                return pair[0], None
        else:
            return pair[0]

    return itertools.imap(helper, iterable)
