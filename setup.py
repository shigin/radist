from distutils.core import setup
try:
    import xpkg
except ImportError:
    pass

setup(
    name="radist",
    version="0.2.3",
    packages=['radist'],
    author="Alexander Shigin",
    author_email='shigin@rambler-co.ru',
    description="Helper module to work with radist config",
    long_description="""Module was developed to easy access to radist 
    configuration file.
    """,
    classifiers=['misc'],
    options={'build_pkg': {'name_prefix': True}},
)
