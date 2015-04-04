##
# python.functools
##
import sys
from .decorlib import method

def rsetattr(attr, val, ob):
	"""
	setattr() and return `ob`. Different order used to allow easier partial
	usage.
	"""
	setattr(ob, attr, val)
	return ob

try:
	from ..port.optimized import rsetattr
except ImportError:
	pass

class Composition(tuple):
	def __call__(self, r):
		for x in self:
			r = x(r)
		return r

	try:
		from ..port.optimized import compose
		__call__ = method(compose)
		del compose
	except ImportError:
		pass

try:
	# C implementation of the tuple processors.
	from ..port.optimized import process_tuple, process_chunk
except ImportError:
	def process_tuple(procs, tup, exception_handler, len = len, tuple = tuple, cause = None):
		"""
		Call each item in `procs` with the corresponding
		item in `tup` returning the result as `type`.

		If an item in `tup` is `None`, don't process it.

		If a give transformation failes, call the given exception_handler.
		"""
		i = len(procs)
		if len(tup) != i:
			raise TypeError(
				"inconsistent items, %d processors and %d items in row" %(
					i, len(tup)
				)
			)
		r = [None] * i
		try:
			for i in range(i):
				ob = tup[i]
				if ob is None:
					continue
				r[i] = procs[i](ob)
		except Exception:
			cause = sys.exc_info()[1]

		if cause is not None:
			exception_handler(cause, procs, tup, i)
			raise RuntimeError("process_tuple exception handler failed to raise")
		return tuple(r)

	def process_chunk(procs, tupc, fail, process_tuple = process_tuple):
		return [process_tuple(procs, x, fail) for x in tupc]
