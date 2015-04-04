##
# .types.io.contrib_hstore - I/O routines for binary hstore
##
from ...python.structlib import split_sized_data, ulong_pack, ulong_unpack
from ...python.itertools import chunk

##
# Build the hstore I/O pair for a given typio.
# It primarily needs typio for decode and encode.
def hstore_factory(oid, typio,
	unpack_err = "expected {0} items in hstore, but found {1}".format
):
	def pack_hstore(x,
		encode = typio.encode,
		len = len,
	):
		if hasattr(x, 'items'):
			x = x.items()
		encoded = [
			(encode(k), encode(v)) if v is not None else (encode(k), None)
			for k,v in x
		]
		return ulong_pack(len(encoded)) + b''.join(
			ulong_pack(len(k)) + k + b'\xFF\xFF\xFF\xFF'
			if v is None else ulong_pack(len(k)) + k + ulong_pack(len(v)) + v
			for k,v in encoded
		)

	def unpack_hstore(x,
		decode = typio.decode,
		split = split_sized_data,
		len = len
	):
		view = memoryview(x)[4:]
		n = ulong_unpack(x)
		r = {
			decode(y[0]) : (decode(y[1]) if y[1] is not None else None)
			for y in chunk(split(view), 2) if y
		}
		if len(r) != n:
			raise ValueError(unpack_err(n, len(r)))
		return r

	return (pack_hstore, unpack_hstore)

oid_to_io = {
	'contrib_hstore' : hstore_factory,
}
