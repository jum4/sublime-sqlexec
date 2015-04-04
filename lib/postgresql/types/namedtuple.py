##
# .types.namedtuple - return rows as namedtuples
##
"""
Factories for namedtuple row representation.
"""
from collections import namedtuple

#: Global namedtuple type cache.
cache = {}

# Build and cache the namedtuple's produced.
def _factory(colnames : [str], namedtuple = namedtuple) -> tuple:
	global cache
	# Provide some normalization.
	# Anything beyond this can just get renamed.
	colnames = tuple([
		x.replace(' ', '_') for x in colnames
	])
	try:
		return cache[colnames]
	except KeyError:
		NT = namedtuple('row', colnames, rename = True)
		cache[colnames] = NT
		return NT

def NamedTupleFactory(attribute_map, composite_relid = None):
	"""
	Alternative db.typio.RowFactory for producing namedtuple's instead of
	postgresql.types.Row() instances.

	To install::

		>>> from postgresql.types.namedtuple import NamedTupleFactory
		>>> import postgresql
		>>> db = postgresql.open(...)
		>>> db.typio.RowTypeFactory(NamedTupleFactory)
	
	And **all** Rows produced by that connection will be namedtuple()'s.
	This includes composites.
	"""
	colnames = list(attribute_map.items())
	colnames.sort(key = lambda x: x[1])
	return lambda y: _factory((x[0] for x in colnames))(*y)

from itertools import chain, starmap

def namedtuples(stmt, from_iter = chain.from_iterable, map = starmap):
	"""
	Alternative to the .rows() execution method.

	Use::
	
		>>> from postgresql.types.namedtuple import namedtuples
		>>> ps = namedtuples(db.prepare(...))
		>>> for nt in ps(...):
		...  nt.a_column_name

	This effectively selects the execution method to be used with the statement.
	"""
	NT = _factory(stmt.column_names)
	# build the execution "method"
	chunks = stmt.chunks
	def rows_as_namedtuples(*args, **kw):
		return map(NT, from_iter(chunks(*args, **kw))) # starmap
	return rows_as_namedtuples

del chain, starmap
