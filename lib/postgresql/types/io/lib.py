import struct
from math import floor
from ...python.functools import Composition as compose
from ...python.itertools import interlace
from ...python.structlib import \
	short_pack, short_unpack, \
	ulong_pack, ulong_unpack, \
	long_pack, long_unpack, \
	double_pack, double_unpack, \
	longlong_pack, longlong_unpack, \
	float_pack, float_unpack, \
	LH_pack, LH_unpack, \
	dl_pack, dl_unpack, \
	dll_pack, dll_unpack, \
	ql_pack, ql_unpack, \
	qll_pack, qll_unpack, \
	llL_pack, llL_unpack, \
	dd_pack, dd_unpack, \
	ddd_pack, ddd_unpack, \
	dddd_pack, dddd_unpack, \
	hhhh_pack, hhhh_unpack

oid_pack = cid_pack = xid_pack = ulong_pack
oid_unpack = cid_unpack = xid_unpack = ulong_unpack
tid_pack, tid_unpack = LH_pack, LH_unpack

# geometry types
point_pack, point_unpack = dd_pack, dd_unpack
circle_pack, circle_unpack = ddd_pack, ddd_unpack
lseg_pack = box_pack = dddd_pack
lseg_unpack = box_unpack = dddd_unpack

null_sequence = b'\xff\xff\xff\xff'
string_format = b'\x00\x00'
binary_format = b'\x00\x01'

def numeric_pack(data, hhhh_pack = hhhh_pack, pack = struct.pack, len = len):
	return hhhh_pack(data[0]) + pack("!%dh"%(len(data[1]),), *data[1])

def numeric_unpack(data, hhhh_unpack = hhhh_unpack, unpack = struct.unpack, len = len):
	return (hhhh_unpack(data[:8]), unpack("!8x%dh"%((len(data)-8) // 2,), data))

def path_pack(data, pack = struct.pack, len = len):
	"""
	Given a sequence of point data, pack it into a path's serialized form.

		[px1, py1, px2, py2, ...]

	Must be an even number of numbers.
	"""
	return pack("!l%dd" %(len(data),), len(data), *data)

def path_unpack(data, long_unpack = long_unpack, unpack = struct.unpack):
	"""
	Unpack a path's serialized form into a sequence of point data:

		[px1, py1, px2, py2, ...]

	Should be an even number of numbers.
	"""
	return unpack("!4x%dd" %(long_unpack(data[:4]),), data)
polygon_pack, polygon_unpack = path_pack, path_unpack

##
# Binary representations of infinity for datetimes.
time_infinity = b'\x7f\xf0\x00\x00\x00\x00\x00\x00'
time_negative_infinity = b'\xff\xf0\x00\x00\x00\x00\x00\x00'
time64_infinity = b'\x7f\xff\xff\xff\xff\xff\xff\xff'
time64_negative_infinity = b'\x80\x00\x00\x00\x00\x00\x00\x00'
date_infinity = b'\x7f\xff\xff\xff'
date_negative_infinity = b'\x80\x00\x00\x00'

# time types
date_pack, date_unpack = long_pack, long_unpack

def mktimetuple(ts, floor = floor):
	'make a pair of (seconds, microseconds) out of the given double'
	seconds = floor(ts)
	return (int(seconds), int(1000000 * (ts - seconds)))

def mktimetuple64(ts, divmod = divmod):
	'make a pair of (seconds, microseconds) out of the given long'
	return divmod(ts, 1000000)

def mktime(seconds_ms, float = float):
	'make a double out of the pair of (seconds, microseconds)'
	return float(seconds_ms[0]) + (seconds_ms[1] / 1000000.0)

def mktime64(seconds_ms):
	'make an integer out of the pair of (seconds, microseconds)'
	return seconds_ms[0] * 1000000 + seconds_ms[1]

# takes a pair, (seconds, microseconds)
time_pack = compose((mktime, double_pack))
time_unpack = compose((double_unpack, mktimetuple))

def interval_pack(m_d_timetup, mktime = mktime, dll_pack = dll_pack):
	"""
	Given a triple, (month, day, (seconds, microseconds)), serialize it for
	transport.
	"""
	(month, day, timetup) = m_d_timetup
	return dll_pack((mktime(timetup), day, month))

def interval_unpack(data, dll_unpack = dll_unpack, mktimetuple = mktimetuple):
	"""
	Given a serialized interval, '{month}{day}{time}', yield the triple:

		(month, day, (seconds, microseconds))
	"""
	tim, day, month = dll_unpack(data)
	return (month, day, mktimetuple(tim))

def interval_noday_pack(month_day_timetup, dl_pack = dl_pack, mktime = mktime):
	"""
	Given a triple, (month, day, (seconds, microseconds)), return the serialized
	form that does not have an individual day component.

	There is no day component, so if day is non-zero, it will be converted to
	seconds and subsequently added to the seconds.
	"""
	(month, day, timetup) = month_day_timetup
	if day:
		timetup = (timetup[0] + (day * 24 * 60 * 60), timetup[1])
	return dl_pack((mktime(timetup), month))

def interval_noday_unpack(data, dl_unpack = dl_unpack, mktimetuple = mktimetuple):
	"""
	Given a serialized interval without a day component, return the triple:

		(month, day(always zero), (seconds, microseconds))
	"""
	tim, month = dl_unpack(data)
	return (month, 0, mktimetuple(tim))

def time64_pack(data, mktime64 = mktime64, longlong_pack = longlong_pack):
	return longlong_pack(mktime64(data))
def time64_unpack(data, longlong_unpack = longlong_unpack, mktimetuple64 = mktimetuple64):
	return mktimetuple64(longlong_unpack(data))

def interval64_pack(m_d_timetup, qll_pack = qll_pack, mktime64 = mktime64):
	"""
	Given a triple, (month, day, (seconds, microseconds)), return the serialized
	data using a quad-word for the (seconds, microseconds) tuple.
	"""
	(month, day, timetup) = m_d_timetup
	return qll_pack((mktime64(timetup), day, month))

def interval64_unpack(data, qll_unpack = qll_unpack, mktimetuple = mktimetuple):
	"""
	Unpack an interval containing a quad-word into a triple:

		(month, day, (seconds, microseconds))
	"""
	tim, day, month = qll_unpack(data)
	return (month, day, mktimetuple64(tim))

def interval64_noday_pack(m_d_timetup, ql_pack = ql_pack, mktime64 = mktime64):
	"""
	Pack an interval without a day component and using a quad-word for second
	representation.

	There is no day component, so if day is non-zero, it will be converted to
	seconds and subsequently added to the seconds.
	"""
	(month, day, timetup) = m_d_timetup
	if day:
		timetup = (timetup[0] + (day * 24 * 60 * 60), timetup[1])
	return ql_pack((mktime64(timetup), month))

def interval64_noday_unpack(data, ql_unpack = ql_unpack, mktimetuple64 = mktimetuple64):
	"""
	Unpack a ``noday`` quad-word based interval. Returns a triple:

		(month, day(always zero), (seconds, microseconds))
	"""
	tim, month = ql_unpack(data)
	return (month, 0, mktimetuple64(tim))

def timetz_pack(timetup_tz, dl_pack = dl_pack, mktime = mktime):
	"""
	Pack a time; offset from beginning of the day and timezone offset.

	Given a pair, ((seconds, microseconds), timezone_offset), pack it into its
	serialized form: "!dl".
	"""
	(timetup, tz_offset) = timetup_tz
	return dl_pack((mktime(timetup), tz_offset))

def timetz_unpack(data, dl_unpack = dl_unpack, mktimetuple = mktimetuple):
	"""
	Given serialized time data, unpack it into a pair:

	    ((seconds, microseconds), timezone_offset).
	"""
	ts, tz = dl_unpack(data)
	return (mktimetuple(ts), tz)

def timetz64_pack(timetup_tz, ql_pack = ql_pack, mktime64 = mktime64):
	"""
	Pack a time; offset from beginning of the day and timezone offset.

	Given a pair, ((seconds, microseconds), timezone_offset), pack it into its
	serialized form using a long long: "!ql".
	"""
	(timetup, tz_offset) = timetup_tz
	return ql_pack((mktime64(timetup), tz_offset))

def timetz64_unpack(data, ql_unpack = ql_unpack, mktimetuple64 = mktimetuple64):
	"""
	Given "long long" serialized time data, "ql", unpack it into a pair:

	    ((seconds, microseconds), timezone_offset)
	"""
	ts, tz = ql_unpack(data)
	return (mktimetuple64(ts), tz)

# oidvectors are 128 bytes, so pack the number of Oids in self
# and justify that to 128 by padding with \x00.
def oidvector_pack(seq, pack = struct.pack):
	"""
	Given a sequence of Oids, pack them into the serialized form.

	An oidvector is a type used by the PostgreSQL catalog.
	"""
	return pack("!%dL"%(len(seq),), *seq).ljust(128, '\x00')

def oidvector_unpack(data, unpack = struct.unpack):
	"""
	Given a serialized oidvector(32 longs), unpack it into a list of unsigned integers.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return unpack("!32L", data)

def int2vector_pack(seq, pack = struct.pack):
	"""
	Given a sequence of integers, pack them into the serialized form.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return pack("!%dh"%(len(seq),), *seq).ljust(64, '\x00')

def int2vector_unpack(data, unpack = struct.unpack):
	"""
	Given a serialized int2vector, unpack it into a list of integers.

	An int2vector is a type used by the PostgreSQL catalog.
	"""
	return unpack("!32h", data)

def varbit_pack(bits_data, long_pack = long_pack):
	r"""
	Given a pair, serialize the varbit.

	# (number of bits, data)
	>>> varbit_pack((1, '\x00'))
	b'\x00\x00\x00\x01\x00'
	"""
	return long_pack(bits_data[0]) + bits_data[1]

def varbit_unpack(data, long_unpack = long_unpack):
	"""
	Given ``varbit`` data, unpack it into a pair:

		(bits, data)

	Where bits are the total number of bits in data (bytes).
	"""
	return long_unpack(data[0:4]), data[4:]

def net_pack(triple,
	# Map PGSQL src/include/utils/inet.h to IP version number.
	fmap = {
		4: 2,
		6: 3,
	},
	len = len,
):
	"""
	net_pack((family, mask, data))

	Pack Postgres' inet/cidr data structure.
	"""
	family, mask, data = triple
	return bytes((fmap[family], mask or 0, 0 if mask is None else 1, len(data))) + data

def net_unpack(data,
	# Map IP version number to PGSQL src/include/utils/inet.h.
	fmap = {
		2: 4,
		3: 6,
	}
):
	"""
	net_unpack(data)

	Unpack Postgres' inet/cidr data structure.
	"""
	family, mask, is_cidr, size = data[:4]
	return (fmap[family], mask, data[4:])

def macaddr_pack(data, bytes = bytes):
	"""
	Pack a MAC address

	Format found in PGSQL src/backend/utils/adt/mac.c, and PGSQL Manual types
	"""
	# Accept all possible PGSQL Macaddr formats as in manual
	# Oh for sscanf() as we could just copy PGSQL C in src/util/adt/mac.c
	colon_parts = data.split(':')
	dash_parts = data.split('-')
	dot_parts = data.split('.')
	if len(colon_parts) == 6:
		mac_parts = colon_parts
	elif len(dash_parts) == 6:
		mac_parts = dash_parts
	elif len(colon_parts) == 2:
		mac_parts = [colon_parts[0][:2], colon_parts[0][2:4], colon_parts[0][4:],
			colon_parts[1][:2], colon_parts[1][2:4], colon_parts[1][4:]]
	elif len(dash_parts) == 2:
		mac_parts = [dash_parts[0][:2], dash_parts[0][2:4], dash_parts[0][4:],
			dash_parts[1][:2], dash_parts[1][2:4], dash_parts[1][4:]]
	elif len(dot_parts) == 3:
		mac_parts = [dot_parts[0][:2], dot_parts[0][2:], dot_parts[1][:2],
			dot_parts[1][2:], dot_parts[2][:2], dot_parts[2][2:]]
	elif len(colon_parts) == 1:
		mac_parts = [data[:2], data[2:4], data[4:6], data[6:8], data[8:10], data[10:]]
	else:
		raise ValueError('data string cannot be parsed to bytes')
	if len(mac_parts) != 6 and len(mac_parts[-1]) != 2:
		raise ValueError('data string cannot be parsed to bytes')
	return bytes([int(p, 16) for p in mac_parts])

def macaddr_unpack(data):
	"""
	Unpack a MAC address

	Format found in PGSQL src/backend/utils/adt/mac.c
	"""
	# This is easy, just go for standard macaddr format,
	# just like PGSQL in src/util/adt/mac.c macaddr_out()
	if len(data) != 6:
		raise ValueError('macaddr has incorrect length')
	return ("%02x:%02x:%02x:%02x:%02x:%02x" % tuple(data))

def record_unpack(data,
	long_unpack = long_unpack,
	oid_unpack = oid_unpack,
	null_sequence = null_sequence,
	len = len):
	"""
	Given serialized record data, return a tuple of tuples of type Oids and
	attributes.
	"""
	columns = long_unpack(data)
	offset = 4

	for x in range(columns):
		typid = oid_unpack(data[offset:offset+4])
		offset += 4

		if data[offset:offset+4] == null_sequence:
			att = None
			offset += 4
		else:
			size = long_unpack(data[offset:offset+4])
			offset += 4
			att = data[offset:offset + size]
			if size < -1 or len(att) != size:
				raise ValueError("insufficient data left in message")
			offset += size
		yield (typid, att)

	if len(data) - offset != 0:
		raise ValueError("extra data, %d octets, at end of record" %(len(data),))

def record_pack(seq,
	long_pack = long_pack,
	oid_pack = oid_pack,
	null_sequence = null_sequence):
	"""
	pack a record given an iterable of (type_oid, data) pairs.
	"""
	return long_pack(len(seq)) + b''.join([
		# typid + (null_seq or data)
		oid_pack(x) + (y is None and null_sequence or (long_pack(len(y)) + y))
		for x, y in seq
	])

def elements_pack(elements,
	null_sequence = null_sequence,
	long_pack = long_pack, len = len
):
	"""
	Pack the elements for containment within a serialized array.

	This is used by array_pack.
	"""
	for x in elements:
		if x is None:
			yield null_sequence
		else:
			yield long_pack(len(x))
			yield x

def array_pack(array_data,
	llL_pack = llL_pack,
	len = len,
	long_pack = long_pack,
	interlace = interlace
):
	"""
	Pack a raw array. A raw array consists of flags, type oid, sequence of lower
	and upper bounds, and an iterable of already serialized element data:

		(0, element type oid, (lower bounds, upper bounds, ...), iterable of element_data)

	The lower bounds and upper bounds specifies boundaries of the dimension. So the length
	of the boundaries sequence is two times the number of dimensions that the array has.

	array_pack((flags, type_id, dims, lowers, element_data))

	The format of ``lower_upper_bounds`` is a sequence of lower bounds and upper
	bounds. First lower then upper inlined within the sequence:

		[lower, upper, lower, upper]

	The above array `dlb` has two dimensions. The lower and upper bounds of the
	first dimension is defined by the first two elements in the sequence. The
	second dimension is then defined by the last two elements in the sequence.
	"""
	(flags, typid, dims, lbs, elements) = array_data
	return llL_pack((len(dims), flags, typid)) + \
		b''.join(map(long_pack, interlace(dims, lbs))) + \
		b''.join(elements_pack(elements))

def elements_unpack(data, offset,
	long_unpack = long_unpack,
	null_sequence = null_sequence):
	"""
	Unpack the serialized elements of an array into a list.

	This is used by array_unpack.
	"""
	data_len = len(data)
	while offset < data_len:
		lend = data[offset:offset+4]
		offset += 4
		if lend == null_sequence:
			yield None
		else:
			sizeof_el = long_unpack(lend)
			yield data[offset:offset+sizeof_el]
			offset += sizeof_el

def array_unpack(data,
	llL_unpack = llL_unpack,
	unpack = struct.unpack_from,
	long_unpack = long_unpack
):
	"""
	Given a serialized array, unpack it into a tuple:

		(flags, typid, (dims, lower bounds, ...), [elements])
	"""
	ndim, flags, typid = llL_unpack(data)
	if ndim < 0:
		raise ValueError("invalid number of dimensions: %d" %(ndim,))
	# "ndim" number of pairs of longs
	end = (4 * 2 * ndim) + 12
	# Dimensions and lower bounds; split the two early.
	#dlb = unpack("!%dl"%(2 * ndim,), data, 12)
	dims = [long_unpack(data[x:x+4]) for x in range(12, end, 8)]
	lbs = [long_unpack(data[x:x+4]) for x in range(16, end, 8)]
	return (flags, typid, dims, lbs, elements_unpack(data, end))
