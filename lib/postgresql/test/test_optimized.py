##
# test.test_optimized
##
import unittest
import struct
import sys
from ..port import optimized
from ..python.itertools import interlace

def pack_tuple(*data,
	packH = struct.Struct("!H").pack,
	packL = struct.Struct("!L").pack
):
	return packH(len(data)) + b''.join((
		packL(len(x)) + x if x is not None else b'\xff\xff\xff\xff'
		for x in data
	))

tuplemessages = (
	(b'D', pack_tuple(b'foo', b'bar')),
	(b'D', pack_tuple(b'foo', None, b'bar')),
	(b'N', b'fee'),
	(b'D', pack_tuple(b'foo', None, b'bar')),
	(b'D', pack_tuple(b'foo', b'bar')),
)

class test_optimized(unittest.TestCase):
	def test_consume_tuple_messages(self):
		ctm = optimized.consume_tuple_messages
		# expecting a tuple of pairs.
		self.assertRaises(TypeError, ctm, [])
		self.assertEqual(ctm(()), [])
		# Make sure that the slicing is working.
		self.assertEqual(ctm(tuplemessages), [
			(b'foo', b'bar'),
			(b'foo', None, b'bar'),
		])
		# Not really checking consume here, but we are validating that
		# it's properly propagating exceptions.
		self.assertRaises(ValueError, ctm, ((b'D', b'\xff\xff\xff\xfefoo'),))
		self.assertRaises(ValueError, ctm, ((b'D', b'\x00\x00\x00\x04foo'),))

	def test_parse_tuple_message(self):
		ptm = optimized.parse_tuple_message
		self.assertRaises(TypeError, ptm, "stringzor")
		self.assertRaises(TypeError, ptm, 123)
		self.assertRaises(ValueError, ptm, b'')
		self.assertRaises(ValueError, ptm, b'0')

		notenoughdata = struct.pack('!H', 2)
		self.assertRaises(ValueError, ptm, notenoughdata)

		wraparound = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 0xFFFFFFFE)
		self.assertRaises(ValueError, ptm, wraparound)

		oneatt_notenough = struct.pack('!HL', 2, 10) + (b'0' * 10) + struct.pack('!L', 15)
		self.assertRaises(ValueError, ptm, oneatt_notenough)

		toomuchdata = struct.pack('!HL', 1, 3) + (b'0' * 10)
		self.assertRaises(ValueError, ptm, toomuchdata)

		class faketup(tuple):
			def __new__(subtype, geeze):
				r = tuple.__new__(subtype, ())
				r.foo = geeze
				return r
		zerodata = struct.pack('!H', 0)
		r = ptm(zerodata)
		self.assertRaises(AttributeError, getattr, r, 'foo')
		self.assertRaises(AttributeError, setattr, r, 'foo', 'bar')
		self.assertEqual(len(r), 0)

	def test_process_tuple(self):
		def funpass(procs, tup, col):
			pass
		pt = optimized.process_tuple
		# tuple() requirements
		self.assertRaises(TypeError, pt, "foo", "bar", funpass)
		self.assertRaises(TypeError, pt, (), "bar", funpass)
		self.assertRaises(TypeError, pt, "foo", (), funpass)
		self.assertRaises(TypeError, pt, (), ("foo",), funpass)

	def test_pack_tuple_data(self):
		pit = optimized.pack_tuple_data
		self.assertEqual(pit((None,)), b'\xff\xff\xff\xff')
		self.assertEqual(pit((None,)*2), b'\xff\xff\xff\xff'*2)
		self.assertEqual(pit((None,)*3), b'\xff\xff\xff\xff'*3)
		self.assertEqual(pit((None,b'foo')), b'\xff\xff\xff\xff\x00\x00\x00\x03foo')
		self.assertEqual(pit((None,b'')), b'\xff\xff\xff\xff\x00\x00\x00\x00')
		self.assertEqual(pit((None,b'',b'bar')), b'\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x03bar')
		self.assertRaises(TypeError, pit, 1)
		self.assertRaises(TypeError, pit, (1,))
		self.assertRaises(TypeError, pit, ("",))

	def test_int2(self):
		d = b'\x00\x01'
		rd = b'\x01\x00'
		s = optimized.swap_int2_unpack(d)
		n = optimized.int2_unpack(d)
		sd = optimized.swap_int2_pack(1)
		nd = optimized.int2_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(1, s)
			self.assertEqual(256, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(1, n)
			self.assertEqual(256, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertRaises(OverflowError, optimized.swap_int2_pack, 2**15)
		self.assertRaises(OverflowError, optimized.int2_pack, 2**15)
		self.assertRaises(OverflowError, optimized.swap_int2_pack, (-2**15)-1)
		self.assertRaises(OverflowError, optimized.int2_pack, (-2**15)-1)

	def test_int4(self):
		d = b'\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00'
		s = optimized.swap_int4_unpack(d)
		n = optimized.int4_unpack(d)
		sd = optimized.swap_int4_pack(1)
		nd = optimized.int4_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(1, s)
			self.assertEqual(16777216, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(1, n)
			self.assertEqual(16777216, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertRaises(OverflowError, optimized.swap_int4_pack, 2**31)
		self.assertRaises(OverflowError, optimized.int4_pack, 2**31)
		self.assertRaises(OverflowError, optimized.swap_int4_pack, (-2**31)-1)
		self.assertRaises(OverflowError, optimized.int4_pack, (-2**31)-1)

	def test_int8(self):
		d = b'\x00\x00\x00\x00\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00\x00\x00\x00\x00'
		s = optimized.swap_int8_unpack(d)
		n = optimized.int8_unpack(d)
		sd = optimized.swap_int8_pack(1)
		nd = optimized.int8_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(0x1, s)
			self.assertEqual(0x100000000000000, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(0x1, n)
			self.assertEqual(0x100000000000000, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertEqual(optimized.swap_int8_pack(-1), b'\xFF\xFF\xFF\xFF'*2)
		self.assertEqual(optimized.int8_pack(-1), b'\xFF\xFF\xFF\xFF'*2)
		self.assertRaises(OverflowError, optimized.swap_int8_pack, 2**63)
		self.assertRaises(OverflowError, optimized.int8_pack, 2**63)
		self.assertRaises(OverflowError, optimized.swap_int8_pack, (-2**63)-1)
		self.assertRaises(OverflowError, optimized.int8_pack, (-2**63)-1)
		# edge I/O
		int8_max = ((2**63) - 1)
		int8_min = (-(2**63))
		swap_max = optimized.swap_int8_pack(int8_max)
		max = optimized.int8_pack(int8_max)
		swap_min = optimized.swap_int8_pack(int8_min)
		min = optimized.int8_pack(int8_min)
		self.assertEqual(optimized.swap_int8_unpack(swap_max), int8_max)
		self.assertEqual(optimized.int8_unpack(max), int8_max)
		self.assertEqual(optimized.swap_int8_unpack(swap_min), int8_min)
		self.assertEqual(optimized.int8_unpack(min), int8_min)

	def test_uint2(self):
		d = b'\x00\x01'
		rd = b'\x01\x00'
		s = optimized.swap_uint2_unpack(d)
		n = optimized.uint2_unpack(d)
		sd = optimized.swap_uint2_pack(1)
		nd = optimized.uint2_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(1, s)
			self.assertEqual(256, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(1, n)
			self.assertEqual(256, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertRaises(OverflowError, optimized.swap_uint2_pack, -1)
		self.assertRaises(OverflowError, optimized.uint2_pack, -1)
		self.assertRaises(OverflowError, optimized.swap_uint2_pack, 2**16)
		self.assertRaises(OverflowError, optimized.uint2_pack, 2**16)
		self.assertEqual(optimized.uint2_pack(2**16-1), b'\xFF\xFF')
		self.assertEqual(optimized.swap_uint2_pack(2**16-1), b'\xFF\xFF')

	def test_uint4(self):
		d = b'\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00'
		s = optimized.swap_uint4_unpack(d)
		n = optimized.uint4_unpack(d)
		sd = optimized.swap_uint4_pack(1)
		nd = optimized.uint4_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(1, s)
			self.assertEqual(16777216, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(1, n)
			self.assertEqual(16777216, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertRaises(OverflowError, optimized.swap_uint4_pack, -1)
		self.assertRaises(OverflowError, optimized.uint4_pack, -1)
		self.assertRaises(OverflowError, optimized.swap_uint4_pack, 2**32)
		self.assertRaises(OverflowError, optimized.uint4_pack, 2**32)
		self.assertEqual(optimized.uint4_pack(2**32-1), b'\xFF\xFF\xFF\xFF')
		self.assertEqual(optimized.swap_uint4_pack(2**32-1), b'\xFF\xFF\xFF\xFF')

	def test_uint8(self):
		d = b'\x00\x00\x00\x00\x00\x00\x00\x01'
		rd = b'\x01\x00\x00\x00\x00\x00\x00\x00'
		s = optimized.swap_uint8_unpack(d)
		n = optimized.uint8_unpack(d)
		sd = optimized.swap_uint8_pack(1)
		nd = optimized.uint8_pack(1)
		if sys.byteorder == 'little':
			self.assertEqual(0x1, s)
			self.assertEqual(0x100000000000000, n)
			self.assertEqual(d, sd)
			self.assertEqual(rd, nd)
		else:
			self.assertEqual(0x1, n)
			self.assertEqual(0x100000000000000, s)
			self.assertEqual(d, nd)
			self.assertEqual(rd, sd)
		self.assertRaises(OverflowError, optimized.swap_uint8_pack, -1)
		self.assertRaises(OverflowError, optimized.uint8_pack, -1)
		self.assertRaises(OverflowError, optimized.swap_uint8_pack, 2**64)
		self.assertRaises(OverflowError, optimized.uint8_pack, 2**64)
		self.assertEqual(optimized.uint8_pack((2**64)-1), b'\xFF\xFF\xFF\xFF'*2)
		self.assertEqual(optimized.swap_uint8_pack((2**64)-1), b'\xFF\xFF\xFF\xFF'*2)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
