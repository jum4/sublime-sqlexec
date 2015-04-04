##
# .test.test_protocol
##
import sys
import unittest
import struct
import decimal
import socket
import time
from threading import Thread

from ..protocol import element3 as e3
from ..protocol import xact3 as x3
from ..protocol import client3 as c3
from ..protocol import buffer as pq_buf
from ..python.socket import find_available_port, SocketFactory

def pair(msg):
	return (msg.type, msg.serialize())
def pairs(*msgseq):
	return list(map(pair, msgseq))

long = struct.Struct("!L")
packl = long.pack
unpackl = long.unpack

class test_buffer(unittest.TestCase):
	def setUp(self):
		self.buffer = pq_buf.pq_message_stream()

	def testMultiByteMessage(self):
		b = self.buffer
		b.write(b's')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x00\x00')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x00\x10')
		self.assertTrue(b.next_message() is None)
		data = b'twelve_chars'
		b.write(data)
		self.assertEqual(b.next_message(), (b's', data))

	def testSingleByteMessage(self):
		b = self.buffer
		b.write(b's')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x00')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x00\x00\x05')
		self.assertTrue(b.next_message() is None)
		b.write(b'b')
		self.assertEqual(b.next_message(), (b's', b'b'))

	def testEmptyMessage(self):
		b = self.buffer
		b.write(b'x')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x00\x00\x00')
		self.assertTrue(b.next_message() is None)
		b.write(b'\x04')
		self.assertEqual(b.next_message(), (b'x', b''))

	def testInvalidLength(self):
		b = self.buffer
		b.write(b'y\x00\x00\x00\x03')
		self.assertRaises(ValueError, b.next_message,)

	def testRemainder(self):
		b = self.buffer
		b.write(b'r\x00\x00\x00\x05Aremainder')
		self.assertEqual(b.next_message(), (b'r', b'A'))

	def testLarge(self):
		b = self.buffer
		factor = 1024
		r = 10000
		b.write(b'X' + packl(factor * r + 4))
		segment = b'\x00' * factor
		for x in range(r-1):
			b.write(segment)
		b.write(segment)
		msg = b.next_message()
		self.assertTrue(msg is not None)
		self.assertEqual(msg[0], b'X')

	def test_getvalue(self):
		# Make sure that getvalue() only applies to messages
		# that have not been read.
		b = self.buffer
		# It should be empty.
		self.assertEqual(b.getvalue(), b'')
		d = b'F' + packl(28)
		b.write(d)
		self.assertEqual(b.getvalue(), d)
		d1 = b'01'*12 # 24
		b.write(d1)
		self.assertEqual(b.getvalue(), d + d1)
		out = b.read()[0]
		self.assertEqual(out, (b'F', d1))
		nd = b'N'
		b.write(nd)
		self.assertEqual(b.getvalue(), nd)
		b.write(packl(4))
		self.assertEqual(list(b.read()), [(b'N', b'')])
		self.assertEqual(b.getvalue(), b'')
		# partial; read one message to exercise
		# that the appropriate fragment of the first
		# chunk in the buffer is picked up.
		first_body = (b'1234' * 3)
		first = b'v' + packl(len(first_body) + 4) + first_body
		second_body = (b'4321' * 5)
		second = b'z' + packl(len(second_body) + 4) + second_body
		b.write(first + second)
		self.assertEqual(b.getvalue(), first + second)
		self.assertEqual(list(b.read(1)), [(b'v', first_body)])
		self.assertEqual(b.getvalue(), second)
		self.assertEqual(list(b.read(1)), [(b'z', second_body)])
		# now, with a third full message in the next chunk
		third_body = (b'9876' * 10)
		third = b'3' + packl(len(third_body) + 4) + third_body
		b.write(first + second)
		b.write(third)
		self.assertEqual(b.getvalue(), first + second + third)
		self.assertEqual(list(b.read(1)), [(b'v', first_body)])
		self.assertEqual(b.getvalue(), second + third)
		self.assertEqual(list(b.read(1)), [(b'z', second_body)])
		self.assertEqual(b.getvalue(), third)
		self.assertEqual(list(b.read(1)), [(b'3', third_body)])
		self.assertEqual(b.getvalue(), b'')

##
# element3 tests
##

message_samples = [
	e3.VoidMessage,
	e3.Startup([
		(b'user', b'jwp'),
		(b'database', b'template1'),
		(b'options', b'-f'),
	]),
	e3.Notice((
		(b'S', b'FATAL'),
		(b'M', b'a descriptive message'),
		(b'C', b'FIVEC'),
		(b'D', b'bleh'),
		(b'H', b'dont spit into the fan'),
	)),
	e3.Notify(123, b'wood_table'),
	e3.KillInformation(19320, 589483),
	e3.ShowOption(b'foo', b'bar'),
	e3.Authentication(4, b'salt'),
	e3.Complete(b'SELECT'),
	e3.Ready(b'I'),
	e3.CancelRequest(4123, 14252),
	e3.NegotiateSSL(),
	e3.Password(b'ckr4t'),
	e3.AttributeTypes(()),
	e3.AttributeTypes(
		(123,) * 1
	),
	e3.AttributeTypes(
		(123,0) * 1
	),
	e3.AttributeTypes(
		(123,0) * 2
	),
	e3.AttributeTypes(
		(123,0) * 4
	),
	e3.TupleDescriptor(()),
	e3.TupleDescriptor((
		(b'name', 123, 1, 1, 0, 0, 1,),
	)),
	e3.TupleDescriptor((
		(b'name', 123, 1, 2, 0, 0, 1,),
	) * 2),
	e3.TupleDescriptor((
		(b'name', 123, 1, 2, 1, 0, 1,),
	) * 3),
	e3.TupleDescriptor((
		(b'name', 123, 1, 1, 0, 0, 1,),
	) * 1000),
	e3.Tuple([]),
	e3.Tuple([b'foo',]),
	e3.Tuple([None]),
	e3.Tuple([b'foo',b'bar']),
	e3.Tuple([None, None]),
	e3.Tuple([None, b'foo', None]),
	e3.Tuple([b'bar', None, b'foo', None, b'bleh']),
	e3.Tuple([b'foo', b'bar'] * 100),
	e3.Tuple([None] * 100),
	e3.Query(b'select * from u'),
	e3.Parse(b'statement_id', b'query', (123, 0)),
	e3.Parse(b'statement_id', b'query', (123,)),
	e3.Parse(b'statement_id', b'query', ()),
	e3.Bind(b'portal_id', b'statement_id',
		(b'tt',b'\x00\x00'),
		[b'data',None], (b'ff',b'xx')),
	e3.Bind(b'portal_id', b'statement_id', (b'tt',), [None], (b'xx',)),
	e3.Bind(b'portal_id', b'statement_id', (b'ff',), [b'data'], ()),
	e3.Bind(b'portal_id', b'statement_id', (), [], (b'xx',)),
	e3.Bind(b'portal_id', b'statement_id', (), [], ()),
	e3.Execute(b'portal_id', 500),
	e3.Execute(b'portal_id', 0),
	e3.DescribeStatement(b'statement_id'),
	e3.DescribePortal(b'portal_id'),
	e3.CloseStatement(b'statement_id'),
	e3.ClosePortal(b'portal_id'),
	e3.Function(123, (), [], b'xx'),
	e3.Function(321, (b'tt',), [b'foo'], b'xx'),
	e3.Function(321, (b'tt',), [None], b'xx'),
	e3.Function(321, (b'aa', b'aa'), [None,b'a' * 200], b'xx'),
	e3.FunctionResult(b''),
	e3.FunctionResult(b'foobar'),
	e3.FunctionResult(None),
	e3.CopyToBegin(123, [321,123]),
	e3.CopyToBegin(0, [10,]),
	e3.CopyToBegin(123, []),
	e3.CopyFromBegin(123, [321,123]),
	e3.CopyFromBegin(0, [10]),
	e3.CopyFromBegin(123, []),
	e3.CopyData(b''),
	e3.CopyData(b'foo'),
	e3.CopyData(b'a' * 2048),
	e3.CopyFail(b''),
	e3.CopyFail(b'iiieeeeee!'),
]

class test_element3(unittest.TestCase):
	def test_cat_messages(self):
		# The optimized implementation will identify adjacent copy data, and
		# take a more efficient route; so rigorously test the switch between the
		# two modes.
		self.assertEqual(e3.cat_messages([]), b'')
		self.assertEqual(e3.cat_messages([b'foo']), b'd\x00\x00\x00\x07foo')
		self.assertEqual(e3.cat_messages([b'foo', b'foo']), 2*b'd\x00\x00\x00\x07foo')
		# copy, other, copy
		self.assertEqual(e3.cat_messages([b'foo', e3.SynchronizeMessage, b'foo']),
			b'd\x00\x00\x00\x07foo' + e3.SynchronizeMessage.bytes() + b'd\x00\x00\x00\x07foo')
		# copy, other, copy*1000
		self.assertEqual(e3.cat_messages(1000*[b'foo', e3.SynchronizeMessage, b'foo']),
			1000*(b'd\x00\x00\x00\x07foo' + e3.SynchronizeMessage.bytes() + b'd\x00\x00\x00\x07foo'))
		# other, copy, copy*1000
		self.assertEqual(e3.cat_messages(1000*[e3.SynchronizeMessage, b'foo', b'foo']),
			1000*(e3.SynchronizeMessage.bytes() + 2*b'd\x00\x00\x00\x07foo'))
		pack_head = struct.Struct("!lH").pack
		# tuple
		self.assertEqual(e3.cat_messages([(b'foo',),]),
			b'D' + pack_head(7 + 4 + 2, 1) + b'\x00\x00\x00\x03foo')
		# tuple(foo,\N)
		self.assertEqual(e3.cat_messages([(b'foo',None,),]),
			b'D' + pack_head(7 + 4 + 4 + 2, 2) + b'\x00\x00\x00\x03foo\xFF\xFF\xFF\xFF')
		# tuple(foo,\N,bar)
		self.assertEqual(e3.cat_messages([(b'foo',None,b'bar'),]),
			b'D' + pack_head(7 + 7 + 4 + 4 + 2, 3) + \
			b'\x00\x00\x00\x03foo\xFF\xFF\xFF\xFF\x00\x00\x00\x03bar')
		# too many attributes
		self.assertRaises((OverflowError, struct.error),
			e3.cat_messages, [(None,) * 0x10000])

		class ThisEx(Exception):
			pass
		class ThatEx(Exception):
			pass
		class Bad(e3.Message):
			def serialize(self):
				raise ThisEx('foo')
		self.assertRaises(ThisEx, e3.cat_messages, [Bad()])
		class NoType(e3.Message):
			def serialize(self):
				return b''
		self.assertRaises(AttributeError, e3.cat_messages, [NoType()])
		class BadType(e3.Message):
			type = 123
			def serialize(self):
				return b''
		self.assertRaises((TypeError,struct.error), e3.cat_messages, [BadType()])


	def testSerializeParseConsistency(self):
		for msg in message_samples:
			smsg = msg.serialize()
			self.assertEqual(msg, msg.parse(smsg))

	def testEmptyMessages(self):
		for x in e3.__dict__.values():
			if isinstance(x, e3.EmptyMessage):
				xtype = type(x)
				self.assertTrue(x is xtype())

	def testUnknownNoticeFields(self):
		N = e3.Notice.parse(b'\x00\x00Z\x00Xklsvdnvldsvkndvlsn\x00Pfoobar\x00Mmessage\x00')
		E = e3.Error.parse(b'Z\x00Xklsvdnvldsvkndvlsn\x00Pfoobar\x00Mmessage\x00\x00')
		self.assertEqual(N[b'M'], b'message')
		self.assertEqual(E[b'M'], b'message')
		self.assertEqual(N[b'P'], b'foobar')
		self.assertEqual(E[b'P'], b'foobar')
		self.assertEqual(len(N), 4)
		self.assertEqual(len(E), 4)

	def testCompleteExtracts(self):
		x = e3.Complete(b'FOO BAR 1321')
		self.assertEqual(x.extract_command(), b'FOO BAR')
		self.assertEqual(x.extract_count(), 1321)
		x = e3.Complete(b' CREATE  	TABLE 13210  ')
		self.assertEqual(x.extract_command(), b'CREATE  	TABLE')
		self.assertEqual(x.extract_count(), 13210)
		x = e3.Complete(b'  CREATE  	TABLE  \t713210  ')
		self.assertEqual(x.extract_command(), b'CREATE  	TABLE')
		self.assertEqual(x.extract_count(), 713210)
		x = e3.Complete(b'  CREATE  	TABLE  0 \t13210  ')
		self.assertEqual(x.extract_command(), b'CREATE  	TABLE')
		self.assertEqual(x.extract_count(), 13210)
		x = e3.Complete(b' 0 \t13210 ')
		self.assertEqual(x.extract_command(), None)
		self.assertEqual(x.extract_count(), 13210)

##
# .protocol.xact3 tests
##

xact_samples = [
	# Simple contrived exchange.
	(
		(
			e3.Query(b"COMPLETE"),
		), (
			e3.Complete(b'COMPLETE'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"ROW DATA"),
		), (
			e3.TupleDescriptor((
				(b'foo', 1, 1, 1, 1, 1, 1),
				(b'bar', 1, 2, 1, 1, 1, 1),
			)),
			e3.Tuple((b'lame', b'lame')),
			e3.Complete(b'COMPLETE'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"ROW DATA"),
		), (
			e3.TupleDescriptor((
				(b'foo', 1, 1, 1, 1, 1, 1),
				(b'bar', 1, 2, 1, 1, 1, 1),
			)),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Tuple((b'lame', b'lame')),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"NULL"),
		), (
			e3.Null(),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Query(b"COPY TO"),
		), (
			e3.CopyToBegin(1, [1,2]),
			e3.CopyData(b'row1'),
			e3.CopyData(b'row2'),
			e3.CopyDone(),
			e3.Complete(b'COPY TO'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Function(1, [b''], [b''], 1),
		), (
			e3.FunctionResult(b'foo'),
			e3.Ready(b'I'),
		)
	),
	(
		(
			e3.Parse(b"NAME", b"SQL", ()),
		), (
			e3.ParseComplete(),
		)
	),
	(
		(
			e3.Bind(b"NAME", b"STATEMENT_ID", (), (), ()),
		), (
			e3.BindComplete(),
		)
	),
	(
		(
			e3.Parse(b"NAME", b"SQL", ()),
			e3.Bind(b"NAME", b"STATEMENT_ID", (), (), ()),
		), (
			e3.ParseComplete(),
			e3.BindComplete(),
		)
	),
	(
		(
			e3.Describe(b"STATEMENT_ID"),
		), (
			e3.AttributeTypes(()),
			e3.NoData(),
		)
	),
	(
		(
			e3.Describe(b"STATEMENT_ID"),
		), (
			e3.AttributeTypes(()),
			e3.TupleDescriptor(()),
		)
	),
	(
		(
			e3.CloseStatement(b"foo"),
		), (
			e3.CloseComplete(),
		),
	),
	(
		(
			e3.ClosePortal(b"foo"),
		), (
			e3.CloseComplete(),
		),
	),
	(
		(
			e3.Synchronize(),
		), (
			e3.Ready(b'I'),
		),
	),
]

class test_xact3(unittest.TestCase):
	def testTransactionSamplesAll(self):
		for xcmd, xres in xact_samples:
			x = x3.Instruction(xcmd)
			r = tuple([(y.type, y.serialize()) for y in xres])
			x.state[1]()
			self.assertEqual(x.messages, ())
			x.state[1](r)
			self.assertEqual(x.state, x3.Complete)
			rec = []
			for y in x.completed:
				for z in y[1]:
					if type(z) is type(b''):
						z = e3.CopyData(z)
					rec.append(z)
			self.assertEqual(xres, tuple(rec))

	def testClosing(self):
		c = x3.Closing()
		self.assertEqual(c.messages, (e3.DisconnectMessage,))
		c.state[1]()
		self.assertEqual(c.fatal, True)
		self.assertEqual(c.error_message.__class__, e3.ClientError)
		self.assertEqual(c.error_message[b'C'], '08003')

	def testNegotiation(self):
		# simple successful run
		n = x3.Negotiation({}, b'')
		n.state[1]()
		n.state[1](
			pairs(
				e3.Notice(((b'M', b"foobar"),)),
				e3.Authentication(e3.AuthRequest_OK, b''),
				e3.KillInformation(0,0),
				e3.ShowOption(b'name', b'val'),
				e3.Ready(b'I'),
			)
		)
		self.assertEqual(n.state, x3.Complete)
		self.assertEqual(n.last_ready.xact_state, b'I')
		# no killinfo.. should cause protocol error...
		n = x3.Negotiation({}, b'')
		n.state[1]()
		n.state[1](
			pairs(
				e3.Notice(((b'M', b"foobar"),)),
				e3.Authentication(e3.AuthRequest_OK, b''),
				e3.ShowOption(b'name', b'val'),
				e3.Ready(b'I'),
			)
		)
		self.assertEqual(n.state, x3.Complete)
		self.assertEqual(n.last_ready, None)
		self.assertEqual(n.error_message[b'C'], '08P01')
		# killinfo twice.. must cause protocol error...
		n = x3.Negotiation({}, b'')
		n.state[1]()
		n.state[1](
			pairs(
				e3.Notice(((b'M', b"foobar"),)),
				e3.Authentication(e3.AuthRequest_OK, b''),
				e3.ShowOption(b'name', b'val'),
				e3.KillInformation(0,0),
				e3.KillInformation(0,0),
				e3.Ready(b'I'),
			)
		)
		self.assertEqual(n.state, x3.Complete)
		self.assertEqual(n.last_ready, None)
		self.assertEqual(n.error_message[b'C'], '08P01')
		# start with ready message..
		n = x3.Negotiation({}, b'')
		n.state[1]()
		n.state[1](
			pairs(
				e3.Notice(((b'M', b"foobar"),)),
				e3.Ready(b'I'),
				e3.Authentication(e3.AuthRequest_OK, b''),
				e3.ShowOption(b'name', b'val'),
			)
		)
		self.assertEqual(n.state, x3.Complete)
		self.assertEqual(n.last_ready, None)
		self.assertEqual(n.error_message[b'C'], '08P01')
		# unsupported authreq
		n = x3.Negotiation({}, b'')
		n.state[1]()
		n.state[1](
			pairs(
				e3.Authentication(255, b''),
			)
		)
		self.assertEqual(n.state, x3.Complete)
		self.assertEqual(n.last_ready, None)
		self.assertEqual(n.error_message[b'C'], '--AUT')

	def testInstructionAsynchook(self):
		l = []
		def hook(data):
			l.append(data)
		x = x3.Instruction([
			e3.Query(b"NOTHING")
		], asynchook = hook)
		a1 = e3.Notice(((b'M', b"m1"),))
		a2 = e3.Notify(0, b'relation', b'parameter')
		a3 = e3.ShowOption(b'optname', b'optval')
		# "send" the query message
		x.state[1]()
		# "receive" the tuple
		x.state[1]([(a1.type, a1.serialize()),])
		a2l = [(a2.type, a2.serialize()),]
		x.state[1](a2l)
		# validate that the hook is not fed twice because
		# it's the exact same message set. (later assertion will validate)
		x.state[1](a2l)
		x.state[1]([(a3.type, a3.serialize()),])
		# we only care about validating that l got everything.
		self.assertEqual([a1,a2,a3], l)
		self.assertEqual(x.state[0], x3.Receiving)
		# validate that the asynchook exception is trapped.
		class Nee(Exception):
			pass
		def ehook(msg):
			raise Nee("this should **not** be part of the summary")
		x = x3.Instruction([
			e3.Query(b"NOTHING")
		], asynchook = ehook)
		a1 = e3.Notice(((b'M', b"m1"),))
		x.state[1]()
		import sys
		v = None
		def exchook(typ, val, tb):
			nonlocal v
			v = val
		seh = sys.excepthook
		sys.excepthook = exchook
		# we only care about validating that the exchook got called.
		x.state[1]([(a1.type, a1.serialize())])
		sys.excepthook = seh
		self.assertTrue(isinstance(v, Nee))

class test_client3(unittest.TestCase):
	def test_timeout(self):
		portnum = find_available_port()
		servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		with servsock:
			servsock.bind(('localhost', portnum))
			pc = c3.Connection(
				SocketFactory(
					(socket.AF_INET, socket.SOCK_STREAM),
					('localhost', portnum)
				),
				{}
			)
			pc.connect(timeout = 1)
			try:
				self.assertEqual(pc.xact.fatal, True)
				self.assertEqual(pc.xact.__class__, x3.Negotiation)
			finally:
				if pc.socket is not None:
					pc.socket.close()

	def test_SSL_failure(self):
		portnum = find_available_port()
		servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		with servsock:
			servsock.bind(('localhost', portnum))
			pc = c3.Connection(
				SocketFactory(
					(socket.AF_INET, socket.SOCK_STREAM),
					('localhost', portnum)
				),
				{}
			)
			exc = None
			servsock.listen(1)
			def client_thread():
				pc.connect(ssl = True)
			client = Thread(target = client_thread)
			try:
				client.start()
				c, addr = servsock.accept()
				with c:
					c.send(b'S')
					c.sendall(b'0000000000000000000000')
					c.recv(1024)
					c.close()
				client.join()
			finally:
				if pc.socket is not None:
					pc.socket.close()

		self.assertEqual(pc.xact.fatal, True)
		self.assertEqual(pc.xact.__class__, x3.Negotiation)
		self.assertEqual(pc.xact.error_message.__class__, e3.ClientError)
		self.assertTrue(hasattr(pc.xact, 'exception'))

	def test_bad_negotiation(self):
		portnum = find_available_port()
		servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		servsock.bind(('localhost', portnum))
		pc = c3.Connection(
			SocketFactory(
				(socket.AF_INET, socket.SOCK_STREAM),
				('localhost', portnum)
			),
			{}
		)
		exc = None
		servsock.listen(1)
		def client_thread():
			pc.connect()
		client = Thread(target = client_thread)
		try:
			client.start()
			c, addr = servsock.accept()
			try:
				c.recv(1024)
			finally:
				c.close()
			time.sleep(0.25)
			client.join()
			servsock.close()
			self.assertEqual(pc.xact.fatal, True)
			self.assertEqual(pc.xact.__class__, x3.Negotiation)
			self.assertEqual(pc.xact.error_message.__class__, e3.ClientError)
			self.assertEqual(pc.xact.error_message[b'C'], '08006')
		finally:
			servsock.close()
			if pc.socket is not None:
				pc.socket.close()

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	try:
		unittest.main(this)
	finally:
		import gc
		gc.collect()
