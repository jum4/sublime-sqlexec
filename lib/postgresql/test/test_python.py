##
# .test.test_python
##
import unittest
import socket
import errno
import struct
from itertools import chain
from operator import methodcaller
from contextlib import contextmanager

from ..python.itertools import interlace
from ..python.structlib import split_sized_data
from ..python import functools
from ..python import itertools
from ..python.socket import find_available_port
from ..python import element

class Ele(element.Element):
	_e_label = property(
		lambda x: getattr(x, 'label', 'ELEMENT')
	)
	_e_factors = ('ancestor', 'secondary')
	secondary = None

	def __init__(self, s = None):
		self.ancestor = s

	def __str__(self):
		return 'STRDATA'

	def _e_metas(self):
		yield ('first', getattr(self, 'first', 'firstv'))
		yield ('second', getattr(self, 'second', 'secondv'))

class test_element(unittest.TestCase):
	def test_primary_factor(self):
		x = Ele()
		# no factors
		self.assertEqual(element.prime_factor(object()), None)
		self.assertEqual(element.prime_factor(x), ('ancestor', None))
		y = Ele(x)
		self.assertEqual(element.prime_factor(y), ('ancestor', x))

	def test_primary_factors(self):
		x = Ele()
		x.ancestor = x
		self.assertRaises(
			element.RecursiveFactor, list, element.prime_factors(x)
		)
		y = Ele(x)
		x.ancestor = y
		self.assertRaises(
			element.RecursiveFactor, list, element.prime_factors(y)
		)
		self.assertRaises(
			element.RecursiveFactor, list, element.prime_factors(x)
		)
		x.ancestor = None
		z = Ele(y)
		self.assertEqual(list(element.prime_factors(z)), [
			('ancestor', y),
			('ancestor', x),
			('ancestor', None),
		])

	def test_format_element(self):
		# Considering that this is subject to change, frequently,
		# I/O equality tests are inappropriate.
		# Rather, a hierarchy will be defined, and the existence
		# of certain pieces of information in the string will be validated.
		x = Ele()
		y = Ele()
		z = Ele()
		alt1 = Ele()
		alt2 = Ele()
		alt1.first = 'alt1-first'
		alt1.second = 'alt1-second'
		alt2.first = 'alt2-first'
		alt2.second = 'alt2-second'
		altprime = Ele()
		altprime.first = 'alt2-ancestor'
		alt2.ancestor = altprime
		z.ancestor = y
		y.ancestor = x
		z.secondary = alt1
		y.secondary = alt2
		x.first = 'unique1'
		y.first = 'unique2'
		x.second = 'unique3'
		z.second = 'unique4'
		y.label = 'DIFF'
		data = element.format_element(z)
		self.assertTrue(x.first in data)
		self.assertTrue(y.first in data)
		self.assertTrue(x.second in data)
		self.assertTrue(z.second in data)
		self.assertTrue('DIFF' in data)
		self.assertTrue('alt1-first' in data)
		self.assertTrue('alt2-first' in data)
		self.assertTrue('alt1-second' in data)
		self.assertTrue('alt2-second' in data)
		self.assertTrue('alt2-ancestor' in data)
		x.ancestor = z
		self.assertRaises(element.RecursiveFactor, element.format_element, z)

class test_itertools(unittest.TestCase):
	def testInterlace(self):
		i1 = range(0, 100, 4)
		i2 = range(1, 100, 4)
		i3 = range(2, 100, 4)
		i4 = range(3, 100, 4)
		self.assertEqual(
			list(itertools.interlace(i1, i2, i3, i4)),
			list(range(100))
		)

class test_functools(unittest.TestCase):
	def testComposition(self):
		compose = functools.Composition
		simple = compose((int, str))
		self.assertEqual("100", simple("100"))
		timesfour_fourtimes = compose((methodcaller('__mul__', 4),)*4)
		self.assertEqual(4*(4*4*4*4), timesfour_fourtimes(4))
		nothing = compose(())
		self.assertEqual(nothing("100"), "100")
		self.assertEqual(nothing(100), 100)
		self.assertEqual(nothing(None), None)

	def testRSetAttr(self):
		class anob(object):
			pass
		ob = anob()
		self.assertRaises(AttributeError, getattr, ob, 'foo')
		rob = functools.rsetattr('foo', 'bar', ob)
		self.assertTrue(rob is ob)
		self.assertTrue(rob.foo is ob.foo)
		self.assertTrue(rob.foo == 'bar')

class test_socket(unittest.TestCase):
	def testFindAvailable(self):
		# the port is randomly generated, so make a few trials before
		# determining success.
		for i in range(100):
			portnum = find_available_port()
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				s.connect(('localhost', portnum))
			except socket.error as err:
				self.assertEqual(err.errno, errno.ECONNREFUSED)
			else:
				self.fail("got a connection to an available port: " + str(portnum))
			finally:
				s.close()

def join_sized_data(*data,
	packL = struct.Struct("!L").pack,
	getlen = lambda x: len(x) if x is not None else 0xFFFFFFFF
):
	return b''.join(interlace(map(packL, map(getlen, data)), (x if x is not None else b'' for x in data)))

class test_structlib(unittest.TestCase):
	def testSizedSplit(self):
		sample = [
			(b'foo', b'bar'),
			(b'foo', None, b'bar'),
			(b'foo', None, b'bar'),
			(b'foo', b'bar'),
			(),
			(None,None,None),
			(b'x', None,None,None, b'yz'),
		]
		packed_sample = [join_sized_data(*x) for x in sample]
		self.assertRaises(ValueError, split_sized_data(b'\xFF\xFF\xFF\x01foo').__next__)
		self.assertEqual(sample, [tuple(split_sized_data(x)) for x in packed_sample])

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
