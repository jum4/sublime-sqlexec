##
# .protocol.element3
##
'PQ version 3.0 elements'
import sys
import os
import pprint
from struct import unpack, Struct
from .message_types import message_types
from ..python.structlib import ushort_pack, ushort_unpack, ulong_pack, ulong_unpack

try:
	from ..port.optimized import parse_tuple_message, pack_tuple_data
except ImportError:
	def pack_tuple_data(atts,
		none = None,
		ulong_pack = ulong_pack,
		blen = bytes.__len__
	):
		return b''.join([
			b'\xff\xff\xff\xff'
			if x is none
			else (ulong_pack(blen(x)) + x)
			for x in atts
		])

try:
	from ..port.optimized import cat_messages
except ImportError:
	from ..python.structlib import lH_pack, long_pack
	# Special case tuple()'s
	def _pack_tuple(t,
		blen = bytes.__len__,
		tlen = tuple.__len__,
		pack_head = lH_pack,
		ulong_pack = ulong_pack,
		ptd = pack_tuple_data,
	):
		# NOTE: duplicated from above
		r = b''.join([
			b'\xff\xff\xff\xff'
			if x is None
			else (ulong_pack(blen(x)) + x)
			for x in t
		])
		return pack_head((blen(r) + 6, tlen(t))) + r

	def cat_messages(messages,
		lpack = long_pack,
		blen = bytes.__len__,
		tuple = tuple,
		pack_tuple = _pack_tuple
	):
		return b''.join([
			(x.bytes() if x.__class__ is not bytes else (
				b'd' + lpack(blen(x) + 4) + x
			)) if x.__class__ is not tuple else (
				b'D' + pack_tuple(x)
			) for x in messages
		])
	del _pack_tuple, lH_pack, long_pack

StringFormat = b'\x00\x00'
BinaryFormat = b'\x00\x01'

class Message(object):
	bytes_struct = Struct("!cL")
	__slots__ = ()
	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			', '.join([repr(getattr(self, x)) for x in self.__slots__])
		)

	def __eq__(self, ob):
		return isinstance(ob, type(self)) and self.type == ob.type and \
		not False in (
			getattr(self, x) == getattr(ob, x)
			for x in self.__slots__
		)

	def bytes(self):
		data = self.serialize()
		return self.bytes_struct.pack(self.type, len(data) + 4) + data

	@classmethod
	def parse(typ, data):
		return typ(data)

class StringMessage(Message):
	"""
	A message based on a single string component.
	"""
	type = b''
	__slots__ = ('data',)

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			repr(self.data),
		)

	def __getitem__(self, i):
		return self.data.__getitem__(i)

	def __init__(self, data):
		self.data = data

	def serialize(self):
		return bytes(self.data) + b'\x00'

	@classmethod
	def parse(typ, data):
		if not data.endswith(b'\x00'):
			raise ValueError("string message not NUL-terminated")
		return typ(data[:-1])

class TupleMessage(tuple, Message):
	"""
	A message who's data is based on a tuple structure.
	"""
	type = b''
	__slots__ = ()

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self)
		)

class Void(Message):
	"""
	An absolutely empty message. When serialized, it always yields an empty
	string.
	"""
	type = b''
	__slots__ = ()

	def bytes(self):
		return b''

	def serialize(self):
		return b''
	
	def __new__(typ, *args, **kw):
		return VoidMessage
VoidMessage = Message.__new__(Void)

def dict_message_repr(self):
	return '%s.%s(**%s)' %(
		type(self).__module__,
		type(self).__name__,
		pprint.pformat(dict(self))
	)

class WireMessage(Message):
	def __init__(self, typ_data):
		self.type = message_types[typ_data[0][0]]
		self.data = typ_data[1]

	def serialize(self):
		return self[1]

	@classmethod
	def parse(typ, data):
		if ulong_unpack(data[1:5]) != len(data) - 1:
			raise ValueError(
				"invalid wire message where data is %d bytes and " \
				"internal size stamp is %d bytes" %(
					len(data), ulong_unpack(data[1:5]) + 1
				)
			)
		return typ((data[0:1], data[5:]))

class EmptyMessage(Message):
	'An abstract message that is always empty'
	__slots__ = ()
	type = b''

	def __new__(typ):
		return typ.SingleInstance

	def serialize(self):
		return b''

	@classmethod
	def parse(typ, data):
		if data != b'':
			raise ValueError("empty message(%r) had data" %(typ.type,))
		return typ.SingleInstance

class Notify(Message):
	'Asynchronous notification message'
	type = message_types[b'A'[0]]
	__slots__ = ('pid', 'channel', 'payload',)

	def __init__(self, pid, channel, payload = b''):
		self.pid = pid
		self.channel = channel
		self.payload = payload

	def serialize(self):
		return ulong_pack(self.pid) + \
			self.channel + b'\x00' + \
			self.payload + b'\x00'

	@classmethod
	def parse(typ, data):
		pid = ulong_unpack(data)
		channel, payload, _ = data[4:].split(b'\x00', 2)
		return typ(pid, channel, payload)

class ShowOption(Message):
	"""ShowOption(name, value)
	GUC variable information from backend"""
	type = message_types[b'S'[0]]
	__slots__ = ('name', 'value')

	def __init__(self, name, value):
		self.name = name
		self.value = value

	def serialize(self):
		return self.name + b'\x00' + self.value + b'\x00'

	@classmethod
	def parse(typ, data):
		return typ(*(data.split(b'\x00', 2)[0:2]))

class Complete(StringMessage):
	'Command completion message.'
	type = message_types[b'C'[0]]
	__slots__ = ()

	@classmethod
	def parse(typ, data):
		return typ(data.rstrip(b'\x00'))

	def extract_count(self):
		"""
		Extract the last set of digits as an integer.
		"""
		# Find the last sequence of digits.
		# If there are no fields consisting only of digits, there is no count.
		for x in reversed(self.data.split()):
			if x.isdigit():
				return int(x)
		return None

	def extract_command(self):
		"""
		Strip all the *surrounding* digits and spaces from the command tag,
		and return that string.
		"""
		return self.data.strip(b'\c\n\t 0123456789') or None

class Null(EmptyMessage):
	'Null command'
	type = message_types[b'I'[0]]
	__slots__ = ()
NullMessage = Message.__new__(Null)
Null.SingleInstance = NullMessage

class NoData(EmptyMessage):
	'Null command'
	type = message_types[b'n'[0]]
	__slots__ = ()
NoDataMessage = Message.__new__(NoData)
NoData.SingleInstance = NoDataMessage

class ParseComplete(EmptyMessage):
	'Parse reaction'
	type = message_types[b'1'[0]]
	__slots__ = ()
ParseCompleteMessage = Message.__new__(ParseComplete)
ParseComplete.SingleInstance = ParseCompleteMessage

class BindComplete(EmptyMessage):
	'Bind reaction'
	type = message_types[b'2'[0]]
	__slots__ = ()
BindCompleteMessage = Message.__new__(BindComplete)
BindComplete.SingleInstance = BindCompleteMessage

class CloseComplete(EmptyMessage):
	'Close statement or Portal'
	type = message_types[b'3'[0]]
	__slots__ = ()
CloseCompleteMessage = Message.__new__(CloseComplete)
CloseComplete.SingleInstance = CloseCompleteMessage

class Suspension(EmptyMessage):
	'Portal was suspended, more tuples for reading'
	type = message_types[b's'[0]]
	__slots__ = ()
SuspensionMessage = Message.__new__(Suspension)
Suspension.SingleInstance = SuspensionMessage

class Ready(Message):
	'Ready for new query'
	type = message_types[b'Z'[0]]
	possible_states = (
		message_types[b'I'[0]],
		message_types[b'E'[0]],
		message_types[b'T'[0]],
	)
	__slots__ = ('xact_state',)

	def __init__(self, data):
		if data not in self.possible_states:
			raise ValueError("invalid state for Ready message: " + repr(data))
		self.xact_state = data

	def serialize(self):
		return self.xact_state

class Notice(Message, dict):
	"""
	Notification message

	Used by PQ to emit INFO, NOTICE, and WARNING messages among other
	severities.
	"""
	type = message_types[b'N'[0]]
	__slots__ = ()
	__repr__ = dict_message_repr

	def serialize(self):
		return b'\x00'.join([
			k + v for k, v in self.items()
			if k and v is not None
		]) + b'\x00'

	@classmethod
	def parse(typ, data, msgtypes = message_types):
		return typ([
			(msgtypes[x[0]], x[1:])
			# "if x" reduce empty fields
			for x in data.split(b'\x00') if x
		])

class ClientNotice(Notice):
	__slots__ = ()

	def serialize(self):
		raise RuntimeError("cannot serialize ClientNotice")

	@classmethod
	def parse(self):
		raise RuntimeError("cannot parse ClientNotice")

class Error(Notice):
	"""Incoming error"""
	type = message_types[b'E'[0]]
	__slots__ = ()

class ClientError(Error):
	__slots__ = ()

	def serialize(self):
		raise RuntimeError("cannot serialize ClientError")

	@classmethod
	def parse(self):
		raise RuntimeError("cannot serialize ClientError")

class FunctionResult(Message):
	"""Function result value"""
	type = message_types[b'V'[0]]
	__slots__ = ('result',)

	def __init__(self, datum):
		self.result = datum

	def serialize(self):
		return self.result is None and b'\xff\xff\xff\xff' or \
			ulong_pack(len(self.result)) + self.result

	@classmethod
	def parse(typ, data):
		if data == b'\xff\xff\xff\xff':
			return typ(None)
		size = ulong_unpack(data[0:4])
		data = data[4:]
		if size != len(data):
			raise ValueError(
				"data length(%d) is not equal to the specified message size(%d)" %(
					len(data), size
				)
			)
		return typ(data)

class AttributeTypes(TupleMessage):
	"""Tuple attribute types"""
	type = message_types[b't'[0]]
	__slots__ = ()

	def serialize(self):
		return ushort_pack(len(self)) + b''.join([ulong_pack(x) for x in self])

	@classmethod
	def parse(typ, data):
		ac = ushort_unpack(data[0:2])
		args = data[2:]
		if len(args) != ac * 4:
			raise ValueError("invalid argument type data size")
		return typ(unpack('!%dL'%(ac,), args))

class TupleDescriptor(TupleMessage):
	"""Tuple description"""
	type = message_types[b'T'[0]]
	struct = Struct("!LhLhlh")
	__slots__ = ()

	def keys(self):
		return [x[0] for x in self]

	def serialize(self):
		return ushort_pack(len(self)) + b''.join([
			x[0] + b'\x00' + self.struct.pack(*x[1:])
			for x in self
		])

	@classmethod
	def parse(typ, data):
		ac = ushort_unpack(data[0:2])
		atts = []
		data = data[2:]
		ca = 0
		while ca < ac:
			# End Of Attribute Name
			eoan = data.index(b'\x00')
			name = data[0:eoan]
			data = data[eoan+1:]
			# name, relationId, columnNumber, typeId, typlen, typmod, format
			atts.append((name,) + typ.struct.unpack(data[0:18]))
			data = data[18:]
			ca += 1
		return typ(atts)

class Tuple(TupleMessage):
	"""Incoming tuple"""
	type = message_types[b'D'[0]]
	__slots__ = ()

	def serialize(self):
		return ushort_pack(len(self)) + pack_tuple_data(self)

	@classmethod
	def parse(typ, data,
		T = tuple, ulong_unpack = ulong_unpack,
		len = len
	):
		natts = ushort_unpack(data[0:2])
		atts = []
		offset = 2
		add = atts.append

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong_unpack(size)
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			add(att)
			natts -= 1
		return T(atts)
	try:
		parse = parse_tuple_message
	except NameError:
		# This is an override when port.optimized is available.
		pass

class KillInformation(Message):
	'Backend cancellation information'
	type = message_types[b'K'[0]]
	struct = Struct("!LL")
	__slots__ = ('pid', 'key')

	def __init__(self, pid, key):
		self.pid = pid
		self.key = key

	def serialize(self):
		return self.struct.pack(self.pid, self.key)

	@classmethod
	def parse(typ, data):
		return typ(*typ.struct.unpack(data))

class CancelRequest(KillInformation):
	'Abort the query in the specified backend'
	type = b''
	from .version import CancelRequestCode as version
	packed_version = version.bytes()
	__slots__ = ('pid', 'key')

	def serialize(self):
		return self.packed_version + self.struct.pack(
			self.pid, self.key
		)

	def bytes(self):
		data = self.serialize()
		return ulong_pack(len(data) + 4) + self.serialize()

	@classmethod
	def parse(typ, data):
		if data[0:4] != typ.packed_version:
			raise ValueError("invalid cancel query code")
		return typ(*typ.struct.unpack(data[4:]))

class NegotiateSSL(Message):
	"Discover backend's SSL support"
	type = b''
	from .version import NegotiateSSLCode as version
	packed_version = version.bytes()
	__slots__ = ()

	def __new__(typ):
		return NegotiateSSLMessage

	def bytes(self):
		data = self.serialize()
		return ulong_pack(len(data) + 4) + data

	def serialize(self):
		return self.packed_version

	@classmethod
	def parse(typ, data):
		if data != typ.packed_version:
			raise ValueError("invalid SSL Negotiation code")
		return NegotiateSSLMessage
NegotiateSSLMessage = Message.__new__(NegotiateSSL)

class Startup(Message, dict):
	"""
	Initiate a connection using the given keywords.
	"""
	type = b''
	from postgresql.protocol.version import V3_0 as version
	packed_version = version.bytes()
	__slots__ = ()
	__repr__ = dict_message_repr

	def serialize(self):
		return self.packed_version + b''.join([
			k + b'\x00' + v + b'\x00'
			for k, v in self.items()
			if v is not None
		]) + b'\x00'

	def bytes(self):
		data = self.serialize()
		return ulong_pack(len(data) + 4) + data

	@classmethod
	def parse(typ, data):
		if data[0:4] != typ.packed_version:
			raise ValueError("invalid version code {1}".format(repr(data[0:4])))
		kw = dict()
		key = None
		for value in data[4:].split(b'\x00')[:-2]:
			if key is None:
				key = value
				continue
			kw[key] = value
			key = None
		return typ(kw)

AuthRequest_OK = 0
AuthRequest_Cleartext = 3
AuthRequest_Password = AuthRequest_Cleartext
AuthRequest_Crypt = 4
AuthRequest_MD5 = 5

# Unsupported by pg_protocol.
AuthRequest_KRB4 = 1
AuthRequest_KRB5 = 2
AuthRequest_SCMC = 6
AuthRequest_SSPI = 9
AuthRequest_GSS = 7
AuthRequest_GSSContinue = 8

AuthNameMap = {
	AuthRequest_Password : 'Cleartext',
	AuthRequest_Crypt : 'Crypt',
	AuthRequest_MD5 : 'MD5',

	AuthRequest_KRB4 : 'Kerberos4',
	AuthRequest_KRB5 : 'Kerberos5',
	AuthRequest_SCMC : 'SCM Credential',
	AuthRequest_SSPI : 'SSPI',
	AuthRequest_GSS : 'GSS',
	AuthRequest_GSSContinue : 'GSSContinue',
}

class Authentication(Message):
	"""Authentication(request, salt)"""
	type = message_types[b'R'[0]]
	__slots__ = ('request', 'salt')

	def __init__(self, request, salt):
		self.request = request
		self.salt = salt

	def serialize(self):
		return ulong_pack(self.request) + self.salt

	@classmethod
	def parse(typ, data):
		return typ(ulong_unpack(data[0:4]), data[4:])

class Password(StringMessage):
	'Password supplement'
	type = message_types[b'p'[0]]
	__slots__ = ('data',)

class Disconnect(EmptyMessage):
	'Close the connection'
	type = message_types[b'X'[0]]
	__slots__ = ()
DisconnectMessage = Message.__new__(Disconnect)
Disconnect.SingleInstance = DisconnectMessage

class Flush(EmptyMessage):
	'Flush'
	type = message_types[b'H'[0]]
	__slots__ = ()
FlushMessage = Message.__new__(Flush)
Flush.SingleInstance = FlushMessage

class Synchronize(EmptyMessage):
	'Synchronize'
	type = message_types[b'S'[0]]
	__slots__ = ()
SynchronizeMessage = Message.__new__(Synchronize)
Synchronize.SingleInstance = SynchronizeMessage

class Query(StringMessage):
	"""Execute the query with the given arguments"""
	type = message_types[b'Q'[0]]
	__slots__ = ('data',)

class Parse(Message):
	"""Parse a query with the specified argument types"""
	type = message_types[b'P'[0]]
	__slots__ = ('name', 'statement', 'argtypes')

	def __init__(self, name, statement, argtypes):
		self.name = name
		self.statement = statement
		self.argtypes = argtypes

	@classmethod
	def parse(typ, data):
		name, statement, args = data.split(b'\x00', 2)
		ac = ushort_unpack(args[0:2])
		args = args[2:]
		if len(args) != ac * 4:
			raise ValueError("invalid argument type data")
		at = unpack('!%dL'%(ac,), args)
		return typ(name, statement, at)

	def serialize(self):
		ac = ushort_pack(len(self.argtypes))
		return self.name + b'\x00' + self.statement + b'\x00' + ac + b''.join([
			ulong_pack(x) for x in self.argtypes
		])

class Bind(Message):
	"""
	Bind a parsed statement with the given arguments to a Portal

	Bind(
		name,      # Portal/Cursor identifier
		statement, # Prepared Statement name/identifier
		aformats,  # Argument formats; Sequence of BinaryFormat or StringFormat.
		arguments, # Argument data; Sequence of None or argument data(str).
		rformats,  # Result formats; Sequence of BinaryFormat or StringFormat.
	)
	"""
	type = message_types[b'B'[0]]
	__slots__ = ('name', 'statement', 'aformats', 'arguments', 'rformats')

	def __init__(self, name, statement, aformats, arguments, rformats):
		self.name = name
		self.statement = statement
		self.aformats = aformats
		self.arguments = arguments
		self.rformats = rformats

	def serialize(self, len = len):
		args = self.arguments
		ac = ushort_pack(len(args))
		ad = pack_tuple_data(tuple(args))
		return \
			self.name + b'\x00' + self.statement + b'\x00' + \
			ac + b''.join(self.aformats) + ac + ad + \
			ushort_pack(len(self.rformats)) + b''.join(self.rformats)

	@classmethod
	def parse(typ, message_data):
		name, statement, data = message_data.split(b'\x00', 2)
		ac = ushort_unpack(data[:2])
		offset = 2 + (2 * ac)
		aformats = unpack(("2s" * ac), data[2:offset])

		natts = ushort_unpack(data[offset:offset+2])
		args = list()
		offset += 2

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong_unpack(size)
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			args.append(att)
			natts -= 1

		rfc = ushort_unpack(data[offset:offset+2])
		ao = offset + 2
		offset = ao + (2 * rfc)
		rformats = unpack(("2s" * rfc), data[ao:offset])

		return typ(name, statement, aformats, args, rformats)

class Execute(Message):
	"""Fetch results from the specified Portal"""
	type = message_types[b'E'[0]]
	__slots__ = ('name', 'max')

	def __init__(self, name, max = 0):
		self.name = name
		self.max = max

	def serialize(self):
		return self.name + b'\x00' + ulong_pack(self.max)

	@classmethod
	def parse(typ, data):
		name, max = data.split(b'\x00', 1)
		return typ(name, ulong_unpack(max))

class Describe(StringMessage):
	"""Describe a Portal or Prepared Statement"""
	type = message_types[b'D'[0]]
	__slots__ = ('data',)

	def serialize(self):
		return self.subtype + self.data + b'\x00'

	@classmethod
	def parse(typ, data):
		if data[0:1] != typ.subtype:
			raise ValueError(
				"invalid Describe message subtype, %r; expected %r" %(
					typ.subtype, data[0:1]
				)
			)
		return super().parse(data[1:])

class DescribeStatement(Describe):
	subtype = message_types[b'S'[0]]
	__slots__ = ('data',)

class DescribePortal(Describe):
	subtype = message_types[b'P'[0]]
	__slots__ = ('data',)

class Close(StringMessage):
	"""Generic Close"""
	type = message_types[b'C'[0]]
	__slots__ = ()

	def serialize(self):
		return self.subtype + self.data + b'\x00'

	@classmethod
	def parse(typ, data):
		if data[0:1] != typ.subtype:
			raise ValueError(
				"invalid Close message subtype, %r; expected %r" %(
					typ.subtype, data[0:1]
				)
			)
		return super().parse(data[1:])

class CloseStatement(Close):
	"""Close the specified Statement"""
	subtype = message_types[b'S'[0]]
	__slots__ = ()

class ClosePortal(Close):
	"""Close the specified Portal"""
	subtype = message_types[b'P'[0]]
	__slots__ = ()

class Function(Message):
	"""Execute the specified function with the given arguments"""
	type = message_types[b'F'[0]]
	__slots__ = ('oid', 'aformats', 'arguments', 'rformat')

	def __init__(self, oid, aformats, args, rformat):
		self.oid = oid
		self.aformats = aformats
		self.arguments = args
		self.rformat = rformat

	def serialize(self):
		ac = ushort_pack(len(self.arguments))
		return ulong_pack(self.oid) + \
			ac + b''.join(self.aformats) + \
			ac + pack_tuple_data(tuple(self.arguments)) + self.rformat

	@classmethod
	def parse(typ, data):
		oid = ulong_unpack(data[0:4])

		ac = ushort_unpack(data[4:6])
		offset = 6 + (2 * ac)
		aformats = unpack(("2s" * ac), data[6:offset])

		natts = ushort_unpack(data[offset:offset+2])
		args = list()
		offset += 2

		while natts > 0:
			alo = offset
			offset += 4
			size = data[alo:offset]
			if size == b'\xff\xff\xff\xff':
				att = None
			else:
				al = ulong_unpack(size)
				ao = offset
				offset = ao + al
				att = data[ao:offset]
			args.append(att)
			natts -= 1

		return typ(oid, aformats, args, data[offset:])

class CopyBegin(Message):
	type = None
	struct = Struct("!BH")
	__slots__ = ('format', 'formats')

	def __init__(self, format, formats):
		self.format = format
		self.formats = formats

	def serialize(self):
		return self.struct.pack(self.format, len(self.formats)) + b''.join([
			ushort_pack(x) for x in self.formats
		])

	@classmethod
	def parse(typ, data):
		format, natts = typ.struct.unpack(data[:3])
		formats_str = data[3:]
		if len(formats_str) != natts * 2:
			raise ValueError("number of formats and data do not match up")
		return typ(format, [
			ushort_unpack(formats_str[x:x+2]) for x in range(0, natts * 2, 2)
		])

class CopyToBegin(CopyBegin):
	"""Begin copying to"""
	type = message_types[b'H'[0]]
	__slots__ = ('format', 'formats')

class CopyFromBegin(CopyBegin):
	"""Begin copying from"""
	type = message_types[b'G'[0]]
	__slots__ = ('format', 'formats')

class CopyData(Message):
	type = message_types[b'd'[0]]
	__slots__ = ('data',)

	def __init__(self, data):
		self.data = bytes(data)

	def serialize(self):
		return self.data

	@classmethod
	def parse(typ, data):
		return typ(data)

class CopyFail(StringMessage):
	type = message_types[b'f'[0]]
	__slots__ = ('data',)

class CopyDone(EmptyMessage):
	type = message_types[b'c'[0]]
	__slots__ = ('data',)
CopyDoneMessage = Message.__new__(CopyDone)
CopyDone.SingleInstance = CopyDoneMessage
