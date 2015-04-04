##
# .test.test_types - test type representations and I/O
##
import unittest
import struct
from ..python.functools import process_tuple
from .. import types as pg_types
from ..types.io import lib as typlib
from ..types.io import builtins
from ..types.io.contrib_hstore import hstore_factory
from ..types import Array

class fake_typio(object):
	@staticmethod
	def encode(x):
		return x.encode('utf-8')
	@staticmethod
	def decode(x):
		return x.decode('utf-8')
hstore_pack, hstore_unpack = hstore_factory(0, fake_typio)

# this must pack to that, and
# that must unpack to this
expectation_samples = {
	('bool', lambda x: builtins.bool_pack(x), lambda x: builtins.bool_unpack(x)) : [
		(True, b'\x01'),
		(False, b'\x00'),
	],

	('int2', builtins.int2_pack, builtins.int2_unpack) : [
		(0, b'\x00\x00'),
		(1, b'\x00\x01'),
		(2, b'\x00\x02'),
		(0x0f, b'\x00\x0f'),
		(0xf00, b'\x0f\x00'),
		(0x7fff, b'\x7f\xff'),
		(-0x8000, b'\x80\x00'),
		(-1, b'\xff\xff'),
		(-2, b'\xff\xfe'),
		(-3, b'\xff\xfd'),
	],

	('int4', builtins.int4_pack, builtins.int4_unpack) : [
		(0, b'\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x02'),
		(0x0f, b'\x00\x00\x00\x0f'),
		(0x7fff, b'\x00\x00\x7f\xff'),
		(-0x8000, b'\xff\xff\x80\x00'),
		(0x7fffffff, b'\x7f\xff\xff\xff'),
		(-0x80000000, b'\x80\x00\x00\x00'),
		(-1, b'\xff\xff\xff\xff'),
		(-2, b'\xff\xff\xff\xfe'),
		(-3, b'\xff\xff\xff\xfd'),
	],

	('int8', builtins.int8_pack, builtins.int8_unpack) : [
		(0, b'\x00\x00\x00\x00\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x00\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x00\x00\x00\x00\x02'),
		(0x0f, b'\x00\x00\x00\x00\x00\x00\x00\x0f'),
		(0x7fffffff, b'\x00\x00\x00\x00\x7f\xff\xff\xff'),
		(0x80000000, b'\x00\x00\x00\x00\x80\x00\x00\x00'),
		(-0x80000000, b'\xff\xff\xff\xff\x80\x00\x00\x00'),
		(-1, b'\xff\xff\xff\xff\xff\xff\xff\xff'),
		(-2, b'\xff\xff\xff\xff\xff\xff\xff\xfe'),
		(-3, b'\xff\xff\xff\xff\xff\xff\xff\xfd'),
	],

	('numeric', typlib.numeric_pack, typlib.numeric_unpack) : [
		(((0,0,0,0),[]), b'\x00'*2*4),
		(((0,0,0,0),[1]), b'\x00'*2*4 + b'\x00\x01'),
		(((1,0,0,0),[1]), b'\x00\x01' + b'\x00'*2*3 + b'\x00\x01'),
		(((1,1,1,1),[1]), b'\x00\x01'*4 + b'\x00\x01'),
		(((1,1,1,1),[1,2]), b'\x00\x01'*4 + b'\x00\x01\x00\x02'),
		(((1,1,1,1),[1,2,3]), b'\x00\x01'*4 + b'\x00\x01\x00\x02\x00\x03'),
	],

	('varbit', typlib.varbit_pack, typlib.varbit_unpack) : [
		((0, b'\x00'), b'\x00\x00\x00\x00\x00'),
		((1, b'\x01'), b'\x00\x00\x00\x01\x01'),
		((1, b'\x00'), b'\x00\x00\x00\x01\x00'),
		((2, b'\x00'), b'\x00\x00\x00\x02\x00'),
		((3, b'\x00'), b'\x00\x00\x00\x03\x00'),
		((9, b'\x00\x00'), b'\x00\x00\x00\x09\x00\x00'),
		# More data than necessary, we allow this.
		# Let the user do the necessary check if the cost is worth the benefit.
		((9, b'\x00\x00\x00'), b'\x00\x00\x00\x09\x00\x00\x00'),
	],

	# idk why
	('bytea', builtins.bytea_pack, builtins.bytea_unpack) : [
		(b'foo', b'foo'),
		(b'bar', b'bar'),
		(b'\x00', b'\x00'),
		(b'\x01', b'\x01'),
	],

	('char', builtins.char_pack, builtins.char_unpack) : [
		(b'a', b'a'),
		(b'b', b'b'),
		(b'\x00', b'\x00'),
	],

	('point', typlib.point_pack, typlib.point_unpack) : [
		((1.0, 1.0), b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0), b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00'),
		((-1.0, -1.0),
			b'\xbf\xf0\x00\x00\x00\x00\x00\x00\xbf\xf0\x00\x00\x00\x00\x00\x00'),
	],

	('circle', typlib.circle_pack, typlib.circle_unpack) : [
		((1.0, 1.0, 1.0),
			b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0\x00\x00' \
			b'\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0, 2.0),
			b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00' \
			b'\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x00'),
	],

	('record', typlib.record_pack, typlib.record_unpack) : [
		([], b'\x00\x00\x00\x00'),
		([(0,b'foo')], b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x03foo'),
		([(0,None)], b'\x00\x00\x00\x01\x00\x00\x00\x00\xff\xff\xff\xff'),
		([(15,None)], b'\x00\x00\x00\x01\x00\x00\x00\x0f\xff\xff\xff\xff'),
		([(0xffffffff,None)], b'\x00\x00\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff'),
		([(0,None), (1,b'some')],
		 b'\x00\x00\x00\x02\x00\x00\x00\x00\xff\xff\xff\xff' \
		 b'\x00\x00\x00\x01\x00\x00\x00\x04some'),
	],

	('array', typlib.array_pack, typlib.array_unpack) : [
		([0, 0xf, (1,), (0,), (b'foo',)],
			b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x01' \
			b'\x00\x00\x00\x00\x00\x00\x00\x03foo'
		),
		([0, 0xf, (1,), (0,), (None,)],
			b'\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x0f\x00\x00\x00\x01' \
			b'\x00\x00\x00\x00\xff\xff\xff\xff'
		)
	],

	('hstore', hstore_pack, hstore_unpack) : [
		({}, b'\x00\x00\x00\x00'),
		({'b' : None}, b'\x00\x00\x00\x01\x00\x00\x00\x01b\xff\xff\xff\xff'),
		({'b' : 'k'}, b'\x00\x00\x00\x01\x00\x00\x00\x01b\x00\x00\x00\x01k'),
		({'foo' : 'bar'}, b'\x00\x00\x00\x01\x00\x00\x00\x03foo\x00\x00\x00\x03bar'),
		({'foo' : None}, b'\x00\x00\x00\x01\x00\x00\x00\x03foo\xff\xff\xff\xff'),
	],
}
expectation_samples[('box', typlib.box_pack, typlib.box_unpack)] = \
	expectation_samples[('lseg', typlib.lseg_pack, typlib.lseg_unpack)] = [
		((1.0, 1.0, 1.0, 1.0),
			b'?\xf0\x00\x00\x00\x00\x00\x00?\xf0' \
			b'\x00\x00\x00\x00\x00\x00?\xf0\x00\x00' \
			b'\x00\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((2.0, 2.0, 1.0, 1.0),
			b'@\x00\x00\x00\x00\x00\x00\x00@\x00\x00' \
			b'\x00\x00\x00\x00\x00?\xf0\x00\x00\x00\x00' \
			b'\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
		((-1.0, -1.0, 1.0, 1.0),
			b'\xbf\xf0\x00\x00\x00\x00\x00\x00\xbf\xf0' \
			b'\x00\x00\x00\x00\x00\x00?\xf0\x00\x00\x00' \
			b'\x00\x00\x00?\xf0\x00\x00\x00\x00\x00\x00'),
	]

expectation_samples[('oid', typlib.oid_pack, typlib.oid_unpack)] = \
	expectation_samples[('cid', typlib.cid_pack, typlib.cid_unpack)] = \
	expectation_samples[('xid', typlib.xid_pack, typlib.xid_unpack)] = [
		(0, b'\x00\x00\x00\x00'),
		(1, b'\x00\x00\x00\x01'),
		(2, b'\x00\x00\x00\x02'),
		(0xf, b'\x00\x00\x00\x0f'),
		(0xffffffff, b'\xff\xff\xff\xff'),
		(0x7fffffff, b'\x7f\xff\xff\xff'),
	]

# this must pack and then unpack back into this
consistency_samples = {
	('bool', lambda x: builtins.bool_pack(x), lambda x: builtins.bool_unpack(x)) : [True, False],

	('record', typlib.record_pack, typlib.record_unpack) : [
		[],
		[(0,b'foo')],
		[(0,None)],
		[(15,None)],
		[(0xffffffff,None)],
		[(0,None), (1,b'some')],
		[(0,None), (1,b'some'), (0xffff, b"something_else\x00")],
		[(0,None), (1,b"s\x00me"), (0xffff, b"\x00something_else\x00")],
	],

	('array', typlib.array_pack, typlib.array_unpack) : [
		[0, 0xf, (), (), ()],
		[0, 0xf, (0,), (0,), ()],
		[0, 0xf, (1,), (0,), (b'foo',)],
		[0, 0xf, (1,), (0,), (None,)],
		[0, 0xf, (2,), (0,), (None,None)],
		[0, 0xf, (2,), (0,), (b'foo',None)],
		[0, 0xff, (2,), (0,), (None,b'foo',)],
		[0, 0xffffffff, (3,), (0,), (None,b'foo',None)],
		[1, 0xffffffff, (3,), (0,), (None,b'foo',None)],
		[1, 0xffffffff, (3, 1), (0, 0), (None,b'foo',None)],
		[1, 0xffffffff, (3, 2), (0, 0), (None,b'one',b'foo',b'two',None,b'three')],
	],

	# Just some random data; it's just an integer, so nothing fancy.
	('date', typlib.date_pack, typlib.date_unpack) : [
		123,
		321,
		0x7FFFFFF,
		-0x8000000,
	],

	('point', typlib.point_pack, typlib.point_unpack) : [
		(0, 0),
		(2, 2),
		(-1, -1),
		(-1.5, -1.2),
		(1.5, 1.2),
	],

	('circle', typlib.circle_pack, typlib.circle_unpack) : [
		(0, 0, 0),
		(2, 2, 2),
		(-1, -1, -1),
		(-1.5, -1.2, -1.8),
	],

	('tid', typlib.tid_pack, typlib.tid_unpack) : [
		(0, 0),
		(1, 1),
		(0xffffffff, 0xffff),
		(0, 0xffff),
		(0xffffffff, 0),
		(0xffffffff // 2, 0xffff // 2),
	],
}
__ = {
	('cidr', typlib.net_pack, typlib.net_unpack) : [
		(0, 0, b"\x00\x00\x00\x00"),
		(2, 0, b"\x00" * 4),
		(2, 0, b"\xFF" * 4),
		(2, 32, b"\xFF" * 4),
		(3, 0, b"\x00\x00" * 16),
	],

	('inet', typlib.net_pack, typlib.net_unpack) : [
		(2, 32, b"\x00\x00\x00\x00"),
		(2, 16, b"\x7f\x00\x00\x01"),
		(2, 8, b"\xff\x00\xff\x01"),
		(3, 128, b"\x7f\x00" * 16),
		(3, 64, b"\xff\xff" * 16),
		(3, 32, b"\x00\x00" * 16),
	],
}

consistency_samples[('time', typlib.time_pack, typlib.time_unpack)] = \
consistency_samples[('time64', typlib.time64_pack, typlib.time64_unpack)] = [
	(0, 0),
	(123, 123),
	(0xFFFFFFFF, 999999),
]

# months, days, (seconds, microseconds)
consistency_samples[('interval', typlib.interval_pack, typlib.interval_unpack)] = [
	(0, 0, (0, 0)),
	(1, 0, (0, 0)),
	(0, 1, (0, 0)),
	(1, 1, (0, 0)),
	(0, 0, (0, 10000)),
	(0, 0, (1, 0)),
	(0, 0, (1, 10000)),
	(1, 1, (1, 10000)),
	(100, 50, (1423, 29313))
]

consistency_samples[('timetz', typlib.timetz_pack, typlib.timetz_unpack)] = \
consistency_samples[('timetz', typlib.timetz64_pack, typlib.timetz64_unpack)] = \
	[
		((0, 0), 0),
		((123, 123), 123),
		((0xFFFFFFFF, 999999), -123),
	]

consistency_samples[('oid', typlib.oid_pack, typlib.oid_unpack)] = \
	consistency_samples[('cid', typlib.cid_pack, typlib.cid_unpack)] = \
	consistency_samples[('xid', typlib.xid_pack, typlib.xid_unpack)] = [
	0, 0xffffffff, 0xffffffff // 2, 123, 321, 1, 2, 3
]

consistency_samples[('lseg', typlib.lseg_pack, typlib.lseg_unpack)] = \
	consistency_samples[('box', typlib.box_pack, typlib.box_unpack)] = [
	(1,2,3,4),
	(4,3,2,1),
	(0,0,0,0),
	(-1,-1,-1,-1),
	(-1.2,-1.5,-2.0,4.0)
]

consistency_samples[('path', typlib.path_pack, typlib.path_unpack)] = \
	consistency_samples[('polygon', typlib.polygon_pack, typlib.polygon_unpack)] = [
	(1,2,3,4),
	(4,3,2,1),
	(0,0,0,0),
	(-1,-1,-1,-1),
	(-1.2,-1.5,-2.0,4.0),
]

from types import GeneratorType
def resolve(ob):
	'make sure generators get "tuplified"'
	if type(ob) not in (list, tuple, GeneratorType):
		return ob
	return [resolve(x) for x in ob]

def testExpectIO(self, samples):
	for id, sample in samples.items():
		name, pack, unpack = id

		for (sample_unpacked, sample_packed) in sample:
			pack_trial = pack(sample_unpacked)
			self.assertTrue(
				pack_trial == sample_packed,
				"%s sample: unpacked sample, %r, did not match " \
				"%r when packed, rather, %r" %(
					name, sample_unpacked,
					sample_packed, pack_trial
				)
			)

			sample_unpacked = resolve(sample_unpacked)
			unpack_trial = resolve(unpack(sample_packed))
			self.assertTrue(
				unpack_trial == sample_unpacked,
				"%s sample: packed sample, %r, did not match " \
				"%r when unpacked, rather, %r" %(
					name, sample_packed,
					sample_unpacked, unpack_trial
				)
			)

class test_io(unittest.TestCase):
	def test_process_tuple(self):
		def funpass(cause, procs, tup, col):
			pass
		self.assertEqual(tuple(process_tuple((),(), funpass)), ())
		self.assertEqual(tuple(process_tuple((int,),("100",), funpass)), (100,))
		self.assertEqual(tuple(process_tuple((int,int),("100","200"), funpass)), (100,200))
		self.assertEqual(tuple(process_tuple((int,int),(None,"200"), funpass)), (None,200))
		self.assertEqual(tuple(process_tuple((int,int,int),(None,None,"200"), funpass)), (None,None,200))
		# The exception handler must raise.
		self.assertRaises(RuntimeError, process_tuple, (int,), ("foo",), funpass)

		class ThisError(Exception):
			pass
		data = []
		def funraise(cause, procs, tup, col):
			data.append((procs, tup, col))
			raise ThisError from cause
		self.assertRaises(ThisError, process_tuple, (int,), ("foo",), funraise)
		self.assertEqual(data[0], ((int,), ("foo",), 0))
		del data[0]
		self.assertRaises(ThisError, process_tuple, (int,int), ("100","bar"), funraise)
		self.assertEqual(data[0], ((int,int), ("100","bar"), 1))

	def testExpectations(self):
		'IO tests where the pre-made expected serialized form is compared'
		testExpectIO(self, expectation_samples)

	def testConsistency(self):
		'IO tests where the unpacked source is compared to re-unpacked result'
		for id, sample in consistency_samples.items():
			name, pack, unpack = id
			if pack is not None:
				for x in sample:
					packed = pack(x)
					unpacked = resolve(unpack(packed))
					x = resolve(x)
					self.assertTrue(x == unpacked,
						"inconsistency with %s, %r -> %r -> %r" %(
							name, x, packed, unpacked
						)
					)

	##
	# Further hstore tests.
	def test_hstore(self):
		# Can't do some tests with the consistency checks
		# because we are not using ordered dictionaries.
		self.assertRaises((ValueError, struct.error), hstore_unpack, b'\x00\x00\x00\x00foo')
		self.assertRaises(ValueError, hstore_unpack, b'\x00\x00\x00\x01')
		self.assertRaises(ValueError, hstore_unpack, b'\x00\x00\x00\x02\x00\x00\x00\x01G\x00\x00\x00\x01G')
		sample = [
			([('foo','bar'),('k',None),('zero','heroes')],
				b'\x00\x00\x00\x03\x00\x00\x00\x03foo' + \
				b'\x00\x00\x00\x03bar\x00\x00\x00\x01k\xFF\xFF\xFF\xFF' + \
				b'\x00\x00\x00\x04zero\x00\x00\x00\x06heroes'),
			([('foo',None),('k',None),('zero',None)],
				b'\x00\x00\x00\x03\x00\x00\x00\x03foo' + \
				b'\xff\xff\xff\xff\x00\x00\x00\x01k\xFF\xFF\xFF\xFF' + \
				b'\x00\x00\x00\x04zero\xFF\xFF\xFF\xFF'),
			([], b'\x00\x00\x00\x00'),
		]
		for x in sample:
			src, serialized = x
			self.assertEqual(hstore_pack(src), serialized)
			self.assertEqual(hstore_unpack(serialized), dict(src))

# Make some slices; used by testSlicing
slice_samples = [
	slice(0, None, x+1) for x in range(10)
] + [
	slice(x, None, 1) for x in range(10)
] + [
	slice(None, x, 1) for x in range(10)
] + [
	slice(None, -x, 70) for x in range(10)
] + [
	slice(x+1, x, -1) for x in range(10)
] + [
	slice(x+4, x, -2) for x in range(10)
]

class test_Array(unittest.TestCase):
	def emptyArray(self, a):
		self.assertEqual(len(a), 0)
		self.assertEqual(list(a.elements()), [])
		self.assertEqual(a.dimensions, ())
		self.assertEqual(a.lowerbounds, ())
		self.assertEqual(a.upperbounds, ())
		self.assertRaises(IndexError, a.__getitem__, 0)

	def testArrayInstantiation(self):
		a = Array([])
		self.emptyArray(a)
		# exercise default upper/lower
		a = Array((1,2,3,))
		self.assertEqual((a[0],a[1],a[2]), (1,2,3,))
		# Python interface, Python semantics.
		self.assertRaises(IndexError, a.__getitem__, 3)
		self.assertEqual(a.dimensions, (3,))
		self.assertEqual(a.lowerbounds, (1,))
		self.assertEqual(a.upperbounds, (3,))

	def testNestedArrayInstantiation(self):
		a = Array(([1,2],[3,4]))
		# Python interface, Python semantics.
		self.assertRaises(IndexError, a.__getitem__, 3)
		self.assertEqual(a.dimensions, (2,2,))
		self.assertEqual(a.lowerbounds, (1,1))
		self.assertEqual(a.upperbounds, (2,2))
		self.assertEqual(list(a.elements()), [1,2,3,4])
		self.assertEqual(list(a),
			[
				Array([1, 2]),
				Array([3, 4]),
			]
		)

		a = Array(([[1],[2]],[[3],[4]]))
		self.assertRaises(IndexError, a.__getitem__, 3)
		self.assertEqual(a.dimensions, (2,2,1))
		self.assertEqual(a.lowerbounds, (1,1,1))
		self.assertEqual(a.upperbounds, (2,2,1))
		self.assertEqual(list(a),
			[
				Array([[1], [2]]),
				Array([[3], [4]]),
			]
		)

		self.assertRaises(ValueError, Array, [
			[1], [2,3]
		])
		self.assertRaises(ValueError, Array, [
			[1], []
		])
		self.assertRaises(ValueError, Array, [
			[[1]],
			[[],2]
		])
		self.assertRaises(ValueError, Array, [
			[[[[[1,2,3]]]]],
			[[[[[1,2,3]]]]],
			[[[[[1,2,3]]]]],
			[[[[[2,2]]]]],
		])

	def testSlicing(self):
		elements = [1,2,3,4,5,6,7,8]
		d1 = Array([1,2,3,4,5,6,7,8])
		for x in slice_samples:
			self.assertEqual(
				d1[x], Array(elements[x])
			)
		elements = [[1,2],[3,4],[5,6],[7,8]]
		d2 = Array(elements)
		for x in slice_samples:
			self.assertEqual(
				d2[x], Array(elements[x])
			)
		elements = [
			[[[1,2],[3,4]]],
			[[[5,6],[791,8]]],
			[[[1,2],[333,4]]],
			[[[1,2],[3,4]]],
			[[[5,10],[7,8]]],
			[[[0,6],[7,8]]],
			[[[1,2],[3,4]]],
			[[[5,6],[7,8]]],
		]
		d3 = Array(elements)
		for x in slice_samples:
			self.assertEqual(
				d3[x], Array(elements[x])
			)

	def testFromElements(self):
		a = Array.from_elements(())
		self.emptyArray(a)

		# exercise default upper/lower
		a = Array.from_elements((1,2,3,))
		self.assertEqual((a[0],a[1],a[2]), (1,2,3,))
		# Python interface, Python semantics.
		self.assertRaises(IndexError, a.__getitem__, 3)
		self.assertEqual(a.dimensions, (3,))
		self.assertEqual(a.lowerbounds, (1,))
		self.assertEqual(a.upperbounds, (3,))

		# exercise default upper/lower
		a = Array.from_elements([3,2,1], lowerbounds = (2,), upperbounds = (4,))
		self.assertEqual(a.dimensions, (3,))
		self.assertEqual(a.lowerbounds, (2,))
		self.assertEqual(a.upperbounds, (4,))

	def testEmptyDimension(self):
		self.assertRaises(ValueError,
			Array, [[]]
		)
		self.assertRaises(ValueError,
			Array, [[2],[]]
		)
		self.assertRaises(ValueError,
			Array, [[],[],[]]
		)
		self.assertRaises(ValueError,
			Array, [[2],[3],[]]
		)

	def testExcessive(self):
		# lowerbounds too high for upperbounds
		self.assertRaises(ValueError,
			Array.from_elements, [1], lowerbounds = (2,), upperbounds = (1,)
		)

	def testNegatives(self):
		a = Array.from_elements([0], lowerbounds = (-1,), upperbounds = (-1,))
		self.assertEqual(a[0], 0)
		self.assertEqual(a[-1], 0)
		# upperbounds at zero
		a = Array.from_elements([1,2], lowerbounds = (-1,), upperbounds = (0,))
		self.assertEqual(a[0], 1)
		self.assertEqual(a[1], 2)
		self.assertEqual(a[-2], 1)
		self.assertEqual(a[-1], 2)

	def testGetElement(self):
		a = Array([1,2,3,4])
		self.assertEqual(a.get_element((0,)), 1)
		self.assertEqual(a.get_element((1,)), 2)
		self.assertEqual(a.get_element((2,)), 3)
		self.assertEqual(a.get_element((3,)), 4)
		self.assertEqual(a.get_element((-1,)), 4)
		self.assertEqual(a.get_element((-2,)), 3)
		self.assertEqual(a.get_element((-3,)), 2)
		self.assertEqual(a.get_element((-4,)), 1)
		self.assertRaises(IndexError, a.get_element, (4,))
		a = Array([[1,2],[3,4]])
		self.assertEqual(a.get_element((0,0)), 1)
		self.assertEqual(a.get_element((0,1,)), 2)
		self.assertEqual(a.get_element((1,0,)), 3)
		self.assertEqual(a.get_element((1,1,)), 4)
		self.assertEqual(a.get_element((-1,-1)), 4)
		self.assertEqual(a.get_element((-1,-2,)), 3)
		self.assertEqual(a.get_element((-2,-1,)), 2)
		self.assertEqual(a.get_element((-2,-2,)), 1)
		self.assertRaises(IndexError, a.get_element, (2,0))
		self.assertRaises(IndexError, a.get_element, (1,2))
		self.assertRaises(IndexError, a.get_element, (0,2))

	def testSQLGetElement(self):
		a = Array([1,2,3,4])
		self.assertEqual(a.sql_get_element((1,)), 1)
		self.assertEqual(a.sql_get_element((2,)), 2)
		self.assertEqual(a.sql_get_element((3,)), 3)
		self.assertEqual(a.sql_get_element((4,)), 4)
		self.assertEqual(a.sql_get_element((0,)), None)
		self.assertEqual(a.sql_get_element((5,)), None)
		self.assertEqual(a.sql_get_element((-1,)), None)
		self.assertEqual(a.sql_get_element((-2,)), None)
		self.assertEqual(a.sql_get_element((-3,)), None)
		self.assertEqual(a.sql_get_element((-4,)), None)
		a = Array([[1,2],[3,4]])
		self.assertEqual(a.sql_get_element((1,1)), 1)
		self.assertEqual(a.sql_get_element((1,2,)), 2)
		self.assertEqual(a.sql_get_element((2,1,)), 3)
		self.assertEqual(a.sql_get_element((2,2,)), 4)
		self.assertEqual(a.sql_get_element((3,1)), None)
		self.assertEqual(a.sql_get_element((1,3)), None)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
