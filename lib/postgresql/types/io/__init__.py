##
# .types.io - I/O routines for packing and unpacking data
##
"""
PostgreSQL type I/O routines--packing and unpacking functions.

This package manages the modules providing I/O routines.

The name of the function describes what type the function is intended to be used
on. Normally, the fucntions return a structured form of the serialized data to
be used as a parameter to the creation of a higher level instance. In
particular, most of the functions that deal with time return a pair for
representing the relative offset: (seconds, microseconds). For times, this
provides an abstraction for quad-word based times used by some configurations of
PostgreSQL.
"""
import sys
from itertools import cycle, chain
from ... import types as pg_types

io_modules = {
	'builtins' : (
		pg_types.BOOLOID,
		pg_types.CHAROID,
		pg_types.BYTEAOID,

		pg_types.INT2OID,
		pg_types.INT4OID,
		pg_types.INT8OID,

		pg_types.FLOAT4OID,
		pg_types.FLOAT8OID,
		pg_types.ABSTIMEOID,
	),

	'pg_bitwise': (
		pg_types.BITOID,
		pg_types.VARBITOID,
	),

        'pg_network': (
		pg_types.MACADDROID,
		pg_types.INETOID,
		pg_types.CIDROID,
        ),

        'pg_system': (
		pg_types.OIDOID,
		pg_types.XIDOID,
		pg_types.CIDOID,
		pg_types.TIDOID,
	),

	'pg_geometry': (
		pg_types.POINTOID,
		pg_types.LSEGOID,
		pg_types.BOXOID,
		pg_types.CIRCLEOID,
	),

	'stdlib_datetime' : (
		pg_types.DATEOID,
		pg_types.INTERVALOID,
		pg_types.TIMEOID,
		pg_types.TIMETZOID,
		pg_types.TIMESTAMPOID,
		pg_types.TIMESTAMPTZOID
	),

	'stdlib_decimal' : (
		pg_types.NUMERICOID,
	),

	'stdlib_uuid' : (
		pg_types.UUIDOID,
	),

	'stdlib_xml_etree' : (
		pg_types.XMLOID,
	),

	# Must be db.typio.identify(contrib_hstore = 'hstore')'d
	'contrib_hstore' : (
		'contrib_hstore',
	),
}

# OID -> module name
module_io = dict(
	chain.from_iterable((
		zip(x[1], cycle((x[0],))) for x in io_modules.items()
	))
)

if sys.version_info[:2] < (3,3):
	def load(relmod):
		return __import__(__name__ + '.' + relmod, fromlist = True, level = 1)
else:
	def load(relmod):
		return __import__(relmod, globals = globals(), locals = locals(), fromlist = [''], level = 1)

def resolve(oid):
	io = module_io.get(oid)
	if io is None:
		return None
	if io.__class__ is str:
		module_io.update(load(io).oid_to_io)
		io = module_io[oid]
	return io
