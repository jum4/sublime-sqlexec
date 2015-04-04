##
# .driver package
##
"""
Driver package for providing an interface to a PostgreSQL database.
"""
__all__ = ['connect', 'default']

from .pq3 import Driver
default = Driver()

def connect(*args, **kw):
	'Establish a connection using the default driver.'
	return default.connect(*args, **kw)
