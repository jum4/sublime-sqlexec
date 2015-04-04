##
# .python.doc
##
"""
Documentation Tools.
"""
from operator import attrgetter

class Doc(object):
	"""
	Simple object that sets the __doc__ attribute to the first parameter and
	initializes __annotations__ using keyword arguments.
	"""
	def __init__(self, doc, **annotations):
		self.__doc__ = str(doc)
		self.__annotations__ = annotations

	__str__ = attrgetter('__doc__')
