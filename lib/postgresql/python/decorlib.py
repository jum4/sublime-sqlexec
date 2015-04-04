##
# .python.decorlib
##
"""
common decorators
"""
import os
import types

def propertydoc(ap):
	"""
	Helper function for extracting an `abstractproperty`'s real documentation.
	"""
	doc = ""
	rstr = ""
	if ap.fget:
		ret = ap.fget.__annotations__.get('return')
		if ret is not None:
			rstr = " -> " + repr(ret)
		if ap.fget.__doc__:
			doc += os.linesep*2 + "GET::" + (os.linesep + ' '*4) + (os.linesep + ' '*4).join(
				[x.strip() for x in ap.fget.__doc__.strip().split(os.linesep)]
			)
	if ap.fset and ap.fset.__doc__:
		doc += os.linesep*2 + "SET::" + (os.linesep + ' '*4) + (os.linesep + ' '*4).join(
			[x.strip() for x in ap.fset.__doc__.strip().split(os.linesep)]
		)
	if ap.fdel and ap.fdel.__doc__:
		doc += os.linesep*2 + "DELETE::" + (os.linesep + ' '*4) + (os.linesep + ' '*4).join(
			[x.strip() for x in ap.fdel.__doc__.strip().split(os.linesep)]
		)
	ap.__doc__ = "<no documentation>" if not doc else (
		"Abstract Property" + rstr + doc
	)
	return ap

class method(object):
	__slots__ = ('callable',)
	def __init__(self, callable):
		self.callable = callable
	def __get__(self, val, typ):
		if val is None:
			return self.callable
		return types.MethodType(self.callable, val)
