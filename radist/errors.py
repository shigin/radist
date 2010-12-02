__all__ = ['RadistError', 'RadistConfError', 'RadistPathError', 'IXError', \
        'IXConfError']

class RadistError(Exception):
    "General radist error."
    pass

class RadistConfError(RadistError):
    "Radist config error."
    pass

class RadistPathError(ValueError, RadistError):
    "Invalid path error."
    def __init__(self, msg='', path=''):
        Exception.__init__(self, msg, path)
        self.message = msg
        self.path = path

class IXError(RadistError):
    pass

class IXConfError(IXError, RadistConfError):
    pass
