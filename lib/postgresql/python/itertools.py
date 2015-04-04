##
# .python.itertools
##
"""
itertools extensions
"""
import collections
from itertools import cycle, islice

def interlace(*iters, next = next) -> collections.Iterable:
	"""
	interlace(i1, i2, ..., in) -> (
		i1-0, i2-0, ..., in-0,
		i1-1, i2-1, ..., in-1,
		.
		.
		.
		i1-n, i2-n, ..., in-n,
	)
	"""
	return map(next, cycle([iter(x) for x in iters]))

def chunk(iterable, chunksize = 256):
	"""
	Given an iterable, return an iterable producing chunks of the objects
	produced by the given iterable.

	chunks([o1,o2,o3,o4], chunksize = 2) -> [
		[o1,o2],
		[o3,o4],
	]
	"""
	iterable = iter(iterable)
	last = ()
	lastsize = chunksize
	while lastsize == chunksize:
		last = list(islice(iterable, chunksize))
		lastsize = len(last)
		yield last

def find(iterable, selector):
	"""
	Return the first item in the `iterable` that causes the `selector` to return
	`True`.
	"""
	for x in iterable:
		if selector(x):
			return x
