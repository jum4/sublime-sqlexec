##
# .driver.pq3 - interface to PostgreSQL using PQ v3.0.
##
"""
PG-API interface for PostgreSQL using PQ version 3.0.
"""
import os
import weakref
import socket
from traceback import format_exception
from itertools import repeat, chain, count
from functools import partial
from abc import abstractmethod
from codecs import lookup as lookup_codecs

from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)

from .. import lib as pg_lib

from .. import versionstring as pg_version
from .. import iri as pg_iri
from .. import exceptions as pg_exc
from .. import string as pg_str
from .. import api as pg_api
from .. import message as pg_msg
from ..encodings.aliases import get_python_name
from ..string import quote_ident

from ..python.itertools import interlace, chunk
from ..python.socket import SocketFactory
from ..python.functools import process_tuple, process_chunk
from ..python.functools import Composition as compose

from ..protocol import xact3 as xact
from ..protocol import element3 as element
from ..protocol import client3 as client
from ..protocol.message_types import message_types

from ..notifyman import NotificationManager
from .. import types as pg_types
from ..types import io as pg_types_io
from ..types.io import lib as io_lib

import warnings

# Map element3.Notice field identifiers
# to names used by message.Message.
notice_field_to_name = {
	message_types[b'S'[0]] : 'severity',
	message_types[b'C'[0]] : 'code',
	message_types[b'M'[0]] : 'message',
	message_types[b'D'[0]] : 'detail',
	message_types[b'H'[0]] : 'hint',
	message_types[b'W'[0]] : 'context',
	message_types[b'P'[0]] : 'position',
	message_types[b'p'[0]] : 'internal_position',
	message_types[b'q'[0]] : 'internal_query',
	message_types[b'F'[0]] : 'file',
	message_types[b'L'[0]] : 'line',
	message_types[b'R'[0]] : 'function',
}
del message_types

notice_field_from_name = dict(
	(v, k) for (k, v) in notice_field_to_name.items()
)

could_not_connect = element.ClientError((
	(b'S', 'FATAL'),
	(b'C', '08001'),
	(b'M', "could not establish connection to server"),
))

# generate an id for a client statement or cursor
def ID(s, title = None, IDNS = 'py:'):
	return IDNS + hex(id(s))

def declare_statement_string(
	cursor_id,
	statement_string,
	insensitive = True,
	scroll = True,
	hold = True
):
	s = 'DECLARE ' + cursor_id
	if insensitive is True:
		s += ' INSENSITIVE'
	if scroll is True:
		s += ' SCROLL'
	s += ' CURSOR'
	if hold is True:
		s += ' WITH HOLD'
	else:
		s += ' WITHOUT HOLD'
	return s + ' FOR ' + statement_string

def direction_str_to_bool(str):
	s = str.upper()
	if s == 'FORWARD':
		return True
	elif s == 'BACKWARD':
		return False
	else:
		raise ValueError("invalid direction " + repr(str))

def direction_to_bool(v):
	if isinstance(v, str):
		return direction_str_to_bool(v)
	elif v is not True and v is not False:
		raise TypeError("invalid direction " + repr(v))
	else:
		return v

class TypeIO(pg_api.TypeIO):
	"""
	A class that manages I/O for a given configuration. Normally, a connection
	would create an instance, and configure it based upon the version and
	configuration of PostgreSQL that it is connected to.
	"""
	_e_factors = ('database',)
	strio = (None, None, str)

	def __init__(self, database):
		self.database = database
		self.encoding = None
		strio = self.strio
		self._cache = {
			# Encoded character strings
			pg_types.ACLITEMOID : strio, # No binary functions.
			pg_types.NAMEOID : strio,
			pg_types.BPCHAROID : strio,
			pg_types.VARCHAROID : strio,
			pg_types.CSTRINGOID : strio,
			pg_types.TEXTOID : strio,
			pg_types.REGTYPEOID : strio,
			pg_types.REGPROCOID : strio,
			pg_types.REGPROCEDUREOID : strio,
			pg_types.REGOPEROID : strio,
			pg_types.REGOPERATOROID : strio,
			pg_types.REGCLASSOID : strio,
		}
		self.typinfo = {}
		super().__init__()

	def lookup_type_info(self, typid):
		return self.database.sys.lookup_type(typid)

	def lookup_composite_type_info(self, typid):
		return self.database.sys.lookup_composite(typid)

	def lookup_domain_basetype(self, typid):
		if self.database.version_info[:2] >= (8, 4):
			return self.lookup_domain_basetype_84(typid)

		while typid:
			r = self.database.sys.lookup_basetype(typid)
			if not r[0][0]:
				return typid
			else:
				typid = r[0][0]

	def lookup_domain_basetype_84(self, typid):
		r = self.database.sys.lookup_basetype_recursive(typid)
		return r[0][0]

	def set_encoding(self, value):
		"""
		Set a new client encoding.
		"""
		self.encoding = value.lower().strip()
		enc = get_python_name(self.encoding)
		ci = lookup_codecs(enc or self.encoding)
		self._encode, self._decode, *_ = ci

	def encode(self, string_data):
		return self._encode(string_data)[0]

	def decode(self, bytes_data):
		return self._decode(bytes_data)[0]

	def encodes(self, iter, get0 = get0):
		"""
		Encode the items in the iterable in the configured encoding.
		"""
		return map(compose((self._encode, get0)), iter)

	def decodes(self, iter, get0 = get0):
		"""
		Decode the items in the iterable from the configured encoding.
		"""
		return map(compose((self._decode, get0)), iter)

	def resolve_pack(self, typid):
		return self.resolve(typid)[0] or self.encode

	def resolve_unpack(self, typid):
		return self.resolve(typid)[1] or self.decode

	def attribute_map(self, pq_descriptor):
		return zip(self.decodes(pq_descriptor.keys()), count())

	def sql_type_from_oid(self, oid, qi = quote_ident):
		if oid in pg_types.oid_to_sql_name:
			return pg_types.oid_to_sql_name[oid]
		if oid in self.typinfo:
			nsp, name, *_ = self.typinfo[oid]
			return qi(nsp) + '.' + qi(name)
		name = pg_types.oid_to_name.get(oid)
		if name:
			return 'pg_catalog.%s' % name
		else:
			return None

	def type_from_oid(self, oid):
		if oid in self._cache:
			typ = self._cache[oid][2]
		return typ

	def resolve_descriptor(self, desc, index):
		'create a sequence of I/O routines from a pq descriptor'
		return [
			(self.resolve(x[3]) or (None, None))[index] for x in desc
		]

	# lookup a type's IO routines from a given typid
	def resolve(self,
		typid : "The Oid of the type to resolve pack and unpack routines for.",
		from_resolution_of : \
		"Sequence of typid's used to identify infinite recursion" = (),
		builtins : "types.io.resolve" = pg_types_io.resolve,
		quote_ident = quote_ident
	):
		if from_resolution_of and typid in from_resolution_of:
			raise TypeError(
				"type, %d, is already being looked up: %r" %(
					typid, from_resolution_of
				)
			)
		typid = int(typid)
		typio = None

		if typid in self._cache:
			typio = self._cache[typid]
		else:
			typio = builtins(typid)
			if typio is not None:
				# If typio is a tuple, it's a constant pair: (pack, unpack)
				# otherwise, it's an I/O pair constructor.
				if typio.__class__ is not tuple:
					typio = typio(typid, self)
				self._cache[typid] = typio

		if typio is None:
			# Lookup the type information for the typid as it's not cached.
			##
			ti = self.lookup_type_info(typid)
			if ti is not None:
				typnamespace, typname, typtype, typlen, typelem, typrelid, \
					ae_typid, ae_hasbin_input, ae_hasbin_output = ti
				self.typinfo[typid] = (
					typnamespace, typname, typrelid, int(typelem) if ae_typid else None
				)
				if typrelid:
					# Row type
					#
					# The attribute name map,
					#  column I/O,
					#  column type Oids
					# are needed to build the packing pair.
					attmap = {}
					cio = []
					typids = []
					attnames = []
					i = 0
					for x in self.lookup_composite_type_info(typrelid):
						attmap[x[1]] = i
						attnames.append(x[1])
						if x[2]:
							# This is a domain
							fieldtypid = self.lookup_domain_basetype(x[0])
						else:
							fieldtypid = x[0]
						typids.append(x[0])
						te = self.resolve(
							fieldtypid, list(from_resolution_of) + [typid]
						)
						cio.append((te[0] or self.encode, te[1] or self.decode))
						i += 1
					self._cache[typid] = typio = self.record_io_factory(
						cio, typids, attmap, list(
							map(self.sql_type_from_oid, typids)
						), attnames,
						typrelid,
						quote_ident(typnamespace) + '.' + \
						quote_ident(typname),
					)
				elif ae_typid is not None:
					# resolve the element type and I/O pair
					te = self.resolve(
						int(typelem),
						from_resolution_of = list(from_resolution_of) + [typid]
					) or (None, None)
					typio = self.array_io_factory(
						te[0] or self.encode,
						te[1] or self.decode,
						typelem,
						ae_hasbin_input,
						ae_hasbin_output
					)
					self._cache[typid] = typio
				else:
					typio = None
					if typtype == b'd':
						basetype = self.lookup_domain_basetype(typid)
						typio = self.resolve(
							basetype,
							from_resolution_of = list(from_resolution_of) + [typid]
						)
					elif typtype == b'p' and typnamespace == 'pg_catalog' and typname == 'record':
						# anonymous record type
						typio = self.anon_record_io_factory()

					if not typio:
						typio = self.strio

					self._cache[typid] = typio
			else:
				# Throw warning about type without entry in pg_type?
				typio = self.strio
		return typio

	def identify(self, **identity_mappings):
		"""
		Explicitly designate the I/O handler for the specified type.

		Primarily used in cases involving UDTs.
		"""
		# get them ordered; we process separately, then recombine.
		id = list(identity_mappings.items())
		ios = [pg_types_io.resolve(x[0]) for x in id]
		oids = list(self.database.sys.regtypes([x[1] for x in id]))

		self._cache.update([
			(oid, io if io.__class__ is tuple else io(oid, self))
			for oid, io in zip(oids, ios)
		])

	def array_parts(self, array, ArrayType = pg_types.Array):
		if array.__class__ is not ArrayType:
			# Assume the data is a nested list.
			array = ArrayType(array)
		return (
			array.elements(),
			array.dimensions,
			array.lowerbounds
		)

	def array_from_parts(self, parts, ArrayType = pg_types.Array):
		elements, dimensions, lowerbounds = parts
		return ArrayType.from_elements(
			elements,
			lowerbounds = lowerbounds,
			upperbounds = [x + lb - 1 for x, lb in zip(dimensions, lowerbounds)]
		)

	##
	# array_io_factory - build I/O pair for ARRAYs
	##
	def array_io_factory(
		self,
		pack_element, unpack_element,
		typoid, # array element id
		hasbin_input, hasbin_output,
		array_pack = io_lib.array_pack,
		array_unpack = io_lib.array_unpack,
	):
		packed_typoid = io_lib.ulong_pack(typoid)
		if hasbin_input:
			def pack_an_array(data, get_parts = self.array_parts):
				elements, dimensions, lowerbounds = get_parts(data)
				return array_pack((
					0, # unused flags
					typoid, dimensions, lowerbounds,
					(x if x is None else pack_element(x) for x in elements),
				))
		else:
			# signals string formatting
			pack_an_array = None

		if hasbin_output:
			def unpack_an_array(data, array_from_parts = self.array_from_parts):
				flags, typoid, dims, lbs, elements = array_unpack(data)
				return array_from_parts(((x if x is None else unpack_element(x) for x in elements), dims, lbs))
		else:
			# signals string formatting
			unpack_an_array = None

		return (pack_an_array, unpack_an_array, pg_types.Array)

	def RowTypeFactory(self, attribute_map = {}, _Row = pg_types.Row.from_sequence, composite_relid = None):
		return partial(_Row, attribute_map)

	##
	# record_io_factory - Build an I/O pair for RECORDs
	##
	def record_io_factory(self,
		column_io : "sequence (pack,unpack) tuples corresponding to the columns",
		typids : "sequence of type Oids; index must correspond to the composite's",
		attmap : "mapping of column name to index number",
		typnames : "sequence of sql type names in order",
		attnames : "sequence of attribute names in order",
		composite_relid : "oid of the composite relation",
		composite_name : "the name of the composite type",
		get0 = get0,
		get1 = get1,
		fmt_errmsg = "failed to {0} attribute {1}, {2}::{3}, of composite {4} from wire data".format
	):
		fpack = tuple(map(get0, column_io))
		funpack = tuple(map(get1, column_io))
		row_constructor = self.RowTypeFactory(attribute_map = attmap, composite_relid = composite_relid)

		def raise_pack_tuple_error(cause, procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			self.raise_client_error(element.ClientError((
				(b'C', '--cIO',),
				(b'S', 'ERROR',),
				(b'M', fmt_errmsg('pack', itemnum, attnames[itemnum], typnames[itemnum], composite_name),),
				(b'W', data,),
				(b'P', str(itemnum),)
			)), cause = cause)

		def raise_unpack_tuple_error(cause, procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			self.raise_client_error(element.ClientError((
				(b'C', '--cIO',),
				(b'S', 'ERROR',),
				(b'M', fmt_errmsg('unpack', itemnum, attnames[itemnum], typnames[itemnum], composite_name),),
				(b'W', data,),
				(b'P', str(itemnum),),
			)), cause = cause)

		def unpack_a_record(data,
			unpack = io_lib.record_unpack,
			process_tuple = process_tuple,
			row_constructor = row_constructor
		):
			data = tuple([x[1] for x in unpack(data)])
			return row_constructor(process_tuple(funpack, data, raise_unpack_tuple_error))

		sorted_atts = sorted(attmap.items(), key = get1)
		def pack_a_record(data,
			pack = io_lib.record_pack,
			process_tuple = process_tuple,
		):
			if isinstance(data, dict):
				data = [data.get(k) for k,_ in sorted_atts]
			return pack(
				tuple(zip(
					typids,
					process_tuple(fpack, tuple(data), raise_pack_tuple_error)
				))
			)
		return (pack_a_record, unpack_a_record, tuple)

	def anon_record_io_factory(self):
		def raise_unpack_tuple_error(cause, procs, tup, itemnum):
			data = repr(tup[itemnum])
			if len(data) > 80:
				# Be sure not to fill screen with noise.
				data = data[:75] + ' ...'
			self.raise_client_error(element.ClientError((
				(b'C', '--cIO',),
				(b'S', 'ERROR',),
				(b'M', 'Could not unpack element {} from anonymous record'.format(itemnum)),
				(b'W', data,),
				(b'P', str(itemnum),)
			)), cause = cause)

		def _unpack_record(data, unpack = io_lib.record_unpack, process_tuple = process_tuple):
			record = list(unpack(data))
			coloids = tuple(x[0] for x in record)
			colio = map(self.resolve, coloids)
			column_unpack = tuple(c[1] or self.decode for c in colio)

			data = tuple(x[1] for x in record)

			return process_tuple(column_unpack, data, raise_unpack_tuple_error)

		return (None, _unpack_record)

	def raise_client_error(self, error_message, cause = None, creator = None):
		m = {
			notice_field_to_name[k] : v
			for k, v in error_message.items()
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}
		c = m.pop('code')
		ms = m.pop('message')
		client_error = self.lookup_exception(c)
		client_error = client_error(ms, code = c, details = m, source = 'CLIENT', creator = creator or self.database)
		client_error.database = self.database
		if cause is not None:
			raise client_error from cause
		else:
			raise client_error

	def lookup_exception(self, code, errorlookup = pg_exc.ErrorLookup,):
		return errorlookup(code)

	def lookup_warning(self, code, warninglookup = pg_exc.WarningLookup,):
		return warninglookup(code)

	def raise_server_error(self, error_message, cause = None, creator = None):
		m = dict(self.decode_notice(error_message))
		c = m.pop('code')
		ms = m.pop('message')
		server_error = self.lookup_exception(c)
		server_error = server_error(ms, code = c, details = m, source = 'SERVER', creator = creator or self.database)
		server_error.database = self.database
		if cause is not None:
			raise server_error from cause
		else:
			raise server_error

	def raise_error(self, error_message, ClientError = element.ClientError, **kw):
		if 'creator' not in kw:
			kw['creator'] = getattr(self.database, '_controller', self.database) or self.database

		if error_message.__class__ is ClientError:
			self.raise_client_error(error_message, **kw)
		else:
			self.raise_server_error(error_message, **kw)

	##
	# Used by decode_notice()
	def _decode_failsafe(self, data):
		decode = self._decode
		i = iter(data)
		for x in i:
			try:
				# prematurely optimized for your viewing displeasure.
				v = x[1]
				yield (x[0], decode(v)[0])
				for x in i:
					v = x[1]
					yield (x[0], decode(v)[0])
			except UnicodeDecodeError:
				# Fallback to the bytes representation.
				# This should be sufficiently informative in most cases,
				# and in the cases where it isn't, an element traceback should
				# ultimately yield the pertinent information
				yield (x[0], repr(x[1])[2:-1])

	def decode_notice(self, notice):
		notice = self._decode_failsafe(notice.items())
		return {
			notice_field_to_name[k] : v
			for k, v in notice
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}

	def emit_server_message(self, message, creator = None,
		MessageType = pg_msg.Message
	):
		fields = self.decode_notice(message)
		m = fields.pop('message')
		c = fields.pop('code')

		if fields['severity'].upper() == 'WARNING':
			MessageType = self.lookup_warning(c)

		message = MessageType(m, code = c, details = fields,
			creator = creator, source = 'SERVER')
		message.database = self.database
		message.emit()
		return message

	def emit_client_message(self, message, creator = None,
		MessageType = pg_msg.Message
	):
		fields = {
			notice_field_to_name[k] : v
			for k, v in message.items()
			# don't include unknown messages in this list.
			if k in notice_field_to_name
		}
		m = fields.pop('message')
		c = fields.pop('code')

		if fields['severity'].upper() == 'WARNING':
			MessageType = self.lookup_warning(c)

		message = MessageType(m, code = c, details = fields,
			creator = creator, source = 'CLIENT')
		message.database = self.database
		message.emit()
		return message

	def emit_message(self, message, ClientNotice = element.ClientNotice, **kw):
		if message.__class__ is ClientNotice:
			return self.emit_client_message(message, **kw)
		else:
			return self.emit_server_message(message, **kw)

##
# This class manages all the functionality used to get
# rows from a PostgreSQL portal/cursor.
class Output(object):
	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	closed = False
	cursor_id = None
	statement = None
	parameters = None

	_complete_message = None

	@abstractmethod
	def _init(self):
		"""
		Bind a cursor based on the configured parameters.
		"""
		# The local initialization for the specific cursor.

	def __init__(self, cursor_id, wref = weakref.ref, ID = ID):
		self.cursor_id = cursor_id
		if self.statement is not None:
			stmt = self.statement
			self._output = stmt._output
			self._output_io = stmt._output_io
			self._row_constructor = stmt._row_constructor
			self._output_formats = stmt._output_formats or ()
			self._output_attmap = stmt._output_attmap

		self._pq_cursor_id = self.database.typio.encode(cursor_id)
		# If the cursor's id was generated, it should be garbage collected.
		if cursor_id == ID(self):
			self.database.pq.register_cursor(self, self._pq_cursor_id)
		self._quoted_cursor_id = '"' + cursor_id.replace('"', '""') + '"'
		self._init()

	def __iter__(self):
		return self

	def close(self):
		if self.closed is False:
			self.database.pq.trash_cursor(self._pq_cursor_id)
		self.closed = True

	def _ins(self, *args):
		return xact.Instruction(*args, asynchook = self.database._receive_async)

	def _pq_xp_describe(self):
		return (element.DescribePortal(self._pq_cursor_id),)

	def _pq_xp_bind(self):
		return (
			element.Bind(
				self._pq_cursor_id,
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self.statement._pq_parameters(self.parameters),
				self._output_formats,
			),
		)

	def _pq_xp_fetchall(self):
		return (
			element.Bind(
				b'',
				self.statement._pq_statement_id,
				self.statement._input_formats,
				self.statement._pq_parameters(self.parameters),
				self._output_formats,
			),
			element.Execute(b'', 0xFFFFFFFF),
		)

	def _pq_xp_declare(self):
		return (
			element.Parse(b'', self.database.typio.encode(
					declare_statement_string(
						str(self._quoted_cursor_id),
						str(self.statement.string)
					)
				), ()
			),
			element.Bind(
				b'', b'', self.statement._input_formats,
				self.statement._pq_parameters(self.parameters), ()
			),
			element.Execute(b'', 1),
		)

	def _pq_xp_execute(self, quantity):
		return (
			element.Execute(self._pq_cursor_id, quantity),
		)

	def _pq_xp_fetch(self, direction, quantity):
		##
		# It's an SQL declared cursor, manually construct the fetch commands.
		qstr = "FETCH " + ("FORWARD " if direction else "BACKWARD ")
		if quantity is None:
			qstr = qstr + "ALL IN " + self._quoted_cursor_id
		else:
			qstr = qstr \
				+ str(quantity) + " IN " + self._quoted_cursor_id
		return (
			element.Parse(b'', self.database.typio.encode(qstr), ()),
			element.Bind(b'', b'', (), (), self._output_formats),
			# The "limit" is defined in the fetch query.
			element.Execute(b'', 0xFFFFFFFF),
		)

	def _pq_xp_move(self, position, whence):
		return (
			element.Parse(b'',
				b'MOVE ' + whence + b' ' + position + b' IN ' + \
				self.database.typio.encode(self._quoted_cursor_id),
				()
			),
			element.Bind(b'', b'', (), (), ()),
			element.Execute(b'', 1),
		)

	def _process_copy_chunk(self, x):
		if x:
			if x[0].__class__ is not bytes or x[-1].__class__ is not bytes:
				return [
					y for y in x if y.__class__ is bytes
				]
		return x

	# Process the element.Tuple message in x for column()
	def _process_tuple_chunk_Column(self, x, range = range):
		unpack = self._output_io[0]
		# get the raw data for the first column
		l = [y[0] for y in x]
		# iterate over the range to keep track
		# of which item we're processing.
		r = range(len(l))
		try:
			return [unpack(l[i]) for i in r]
		except Exception:
			cause = sys.exc_info()[1]
		try:
			i = next(r)
		except StopIteration:
			i = len(l)
		self._raise_column_tuple_error(cause, self._output_io, (l[i],), 0)

	# Process the element.Tuple message in x for rows()
	def _process_tuple_chunk_Row(self, x,
		proc = process_chunk,
	):
		rc = self._row_constructor
		return [
			rc(y)
			for y in proc(self._output_io, x, self._raise_column_tuple_error)
		]

	# Process the elemnt.Tuple messages in `x` for chunks()
	def _process_tuple_chunk(self, x, proc = process_chunk):
		return proc(self._output_io, x, self._raise_column_tuple_error)

	def _raise_column_tuple_error(self, cause, procs, tup, itemnum):
		'for column processing'
		# The element traceback will include the full list of parameters.
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'

		em = element.ClientError((
			(b'S', 'ERROR'),
			(b'C', "--CIO"),
			(b'M', "failed to unpack column %r, %s::%s, from wire data" %(
					itemnum,
					self.column_names[itemnum],
					self.database.typio.sql_type_from_oid(
						self.statement.pg_column_types[itemnum]
					) or '<unknown>',
				)
			),
			(b'D', data),
			(b'H', "Try casting the column to 'text'."),
			(b'P', str(itemnum)),
		))
		self.database.typio.raise_client_error(em, creator = self, cause = cause)

	@property
	def state(self):
		if self.closed:
			return 'closed'
		else:
			return 'open'

	@property
	def column_names(self):
		if self._output is not None:
			return list(self.database.typio.decodes(self._output.keys()))
		# `None` if _output does not exist; not row data

	@property
	def column_types(self):
		if self._output is not None:
			return [self.database.typio.type_from_oid(x[3]) for x in self._output]
		# `None` if _output does not exist; not row data

	@property
	def pg_column_types(self):
		if self._output is not None:
			return [x[3] for x in self._output]
		# `None` if _output does not exist; not row data

	@property
	def sql_column_types(self):
		return [
			self.database.typio.sql_type_from_oid(x)
			for x in self.pg_column_types
		]

	def command(self):
		"The completion message's command identifier"
		if self._complete_message is not None:
			return self._complete_message.extract_command().decode('ascii')

	def count(self):
		"The completion message's count number"
		if self._complete_message is not None:
			return self._complete_message.extract_count()

class Chunks(Output, pg_api.Chunks):
	pass

##
# FetchAll - A Chunks cursor that gets *all* the records in the cursor.
#
# It has added complexity over other variants as in order to stream results,
# chunks have to be removed from the protocol transaction's received messages.
# If this wasn't done, the entire result set would be fully buffered prior
# to processing.
class FetchAll(Chunks):
	_e_factors = ('statement', 'parameters',)
	def _e_metas(self):
		yield ('type', type(self).__name__)

	def __init__(self, statement, parameters):
		self.statement = statement
		self.parameters = parameters
		self.database = statement.database
		Output.__init__(self, '')

	def _init(self,
		null = element.Null.type,
		complete = element.Complete.type,
		bindcomplete = element.BindComplete.type,
		parsecomplete = element.ParseComplete.type,
	):
		expect = self._expect
		self._xact = self._ins(
			self._pq_xp_fetchall() + (element.SynchronizeMessage,)
		)
		self.database._pq_push(self._xact, self)

		# Get more messages until the first Tuple is seen.
		STEP = self.database._pq_step
		while self._xact.state != xact.Complete:
			STEP()
			for x in self._xact.messages_received():
				if x.__class__ is tuple or expect == x.type:
					# No need to step anymore once this is seen.
					return
				elif x.type == null:
					# The protocol transaction is going to be complete..
					self.database._pq_complete()
					self._xact = None
					return
				elif x.type == complete:
					self._complete_message = x
					self.database._pq_complete()
					# If this was a select/copy cursor,
					# the data messages would have caused an earlier
					# return. It's empty.
					self._xact = None
					return
				elif x.type in (bindcomplete, parsecomplete):
					# Noise.
					pass
				else:
					# This should have been caught by the protocol transaction.
					# "Can't happen".
					self.database._pq_complete()
					if self._xact.fatal is None:
						self._xact.fatal = False
						self._xact.error_message = element.ClientError((
							(b'S', 'ERROR'),
							(b'C', "--000"),
							(b'M', "unexpected message type " + repr(x.type))
						))
						self.database.typio.raise_client_error(self._xact.error_message, creator = self)
					return

	def __next__(self,
		data_types = (tuple,bytes),
		complete = element.Complete.type,
	):
		x = self._xact
		# self._xact = None; means that the cursor has been exhausted.
		if x is None:
			raise StopIteration

		# Finish the protocol transaction.
		STEP = self.database._pq_step
		while x.state is not xact.Complete and not x.completed:
			STEP()

		# fatal is None == no error
		# fatal is True == dead connection
		# fatal is False == dead transaction
		if x.fatal is not None:
			self.database.typio.raise_error(x.error_message, creator = getattr(self, '_controller', self) or self)

		# no messages to process?
		if not x.completed:
			# Transaction has been cleaned out of completed? iterator is done.
			self._xact = None
			self.close()
			raise StopIteration

		# Get the chunk to be processed.
		chunk = [
			y for y in x.completed[0][1]
			if y.__class__ in data_types
		]
		r = self._process_chunk(chunk)

		# Scan for _complete_message.
		# Arguably, this can fail, but it would be a case
		# where multiple sync messages were issued. Something that's
		# not naturally occurring.
		for y in x.completed[0][1][-3:]:
			if getattr(y, 'type', None) == complete:
				self._complete_message = y

		# Remove it, it's been processed.
		del x.completed[0]
		return r

class SingleXactCopy(FetchAll):
	_expect = element.CopyToBegin.type
	_process_chunk = FetchAll._process_copy_chunk

class SingleXactFetch(FetchAll):
	_expect = element.Tuple.type

class MultiXactStream(Chunks):
	chunksize = 1024 * 4
	# only tuple streams
	_process_chunk = Output._process_tuple_chunk

	def _e_metas(self):
		yield ('chunksize', self.chunksize)
		yield ('type', self.__class__.__name__)

	def __init__(self, statement, parameters, cursor_id):
		self.statement = statement
		self.parameters = parameters
		self.database = statement.database
		Output.__init__(self, cursor_id or ID(self))

	@abstractmethod
	def _bind(self):
		"""
		Generate the commands needed to bind the cursor.
		"""

	@abstractmethod
	def _fetch(self):
		"""
		Generate the commands needed to bind the cursor.
		"""

	def _init(self):
		self._command = self._fetch()
		self._xact = self._ins(self._bind() + self._command)
		self.database._pq_push(self._xact, self)

	def __next__(self, tuple_type = tuple):
		x = self._xact
		if x is None:
			raise StopIteration

		if self.database.pq.xact is x:
			self.database._pq_complete()

		# get all the element.Tuple messages
		chunk = [
			y for y in x.messages_received() if y.__class__ is tuple_type
		]
		if len(chunk) == self.chunksize:
			# there may be more, dispatch the request for the next chunk
			self._xact = self._ins(self._command)
			self.database._pq_push(self._xact, self)
		else:
			# it's done.
			self._xact = None
			self.close()
			if not chunk:
				# chunk is empty, it's done *right* now.
				raise StopIteration
		chunk = self._process_chunk(chunk)
		return chunk

##
# The cursor is streamed to the client on demand *inside*
# a single SQL transaction block.
class MultiXactInsideBlock(MultiXactStream):
	_bind = MultiXactStream._pq_xp_bind
	def _fetch(self):
		##
		# Use the extended protocol's execute to fetch more.
		return self._pq_xp_execute(self.chunksize) + \
			(element.SynchronizeMessage,)

##
# The cursor is streamed to the client on demand *outside* of
# a single SQL transaction block. [DECLARE ... WITH HOLD]
class MultiXactOutsideBlock(MultiXactStream):
	_bind = MultiXactStream._pq_xp_declare

	def _fetch(self):
		##
		# Use the extended protocol's execute to fetch more *against*
		# an SQL FETCH statement yielding the data in the proper format.
		#
		# MultiXactOutsideBlock uses DECLARE to create the cursor WITH HOLD.
		# When this is done, the cursor is configured to use StringFormat with
		# all columns. It's necessary to use FETCH to adjust the formatting.
		return self._pq_xp_fetch(True, self.chunksize) + \
			(element.SynchronizeMessage,)

##
# Cursor is used to manage scrollable cursors.
class Cursor(Output, pg_api.Cursor):
	_process_tuple = Output._process_tuple_chunk_Row
	def _e_metas(self):
		yield ('direction', 'FORWARD' if self.direction else 'BACKWORD')
		yield ('type', 'Cursor')

	def clone(self):
		return type(self)(self.statement, self.parameters, self.database, None)

	def __init__(self, statement, parameters, database, cursor_id):
		self.database = database or statement.database
		self.statement = statement
		self.parameters = parameters
		self.__dict__['direction'] = True
		if self.statement is None:
			self._e_factors = ('database', 'cursor_id')
		Output.__init__(self, cursor_id or ID(self))

	def get_direction(self):
		return self.__dict__['direction']
	def set_direction(self, value):
		self.__dict__['direction'] = direction_to_bool(value)
	direction = property(
		fget = get_direction,
		fset = set_direction,
	)
	del get_direction, set_direction

	def _which_way(self, direction):
		if direction is not None:
			direction = direction_to_bool(direction)
			# -1 * -1 = 1, -1 * 1 = -1, 1 * 1 = 1
			return not ((not self.direction) ^ (not direction))
		else:
			return self.direction

	def _init(self,
		tupledesc = element.TupleDescriptor.type,
	):
		"""
		Based on the cursor parameters and the current transaction state,
		select a cursor strategy for managing the response from the server.
		"""
		if self.statement is not None:
			x = self._ins(self._pq_xp_declare() + (element.SynchronizeMessage,))
			self.database._pq_push(x, self)
			self.database._pq_complete()
		else:
			x = self._ins(self._pq_xp_describe() + (element.SynchronizeMessage,))
			self.database._pq_push(x, self)
			self.database._pq_complete()
			for m in x.messages_received():
				if m.type == tupledesc:
					typio = self.database.typio
					self._output = m
					self._output_attmap = typio.attribute_map(self._output)
					self._row_constructor = typio.RowTypeFactory(self._output_attmap)
					# tuple output
					self._output_io = typio.resolve_descriptor(
						self._output, 1 # (input, output)[1]
					)
					self._output_formats = [
						element.StringFormat
						if x is None
						else element.BinaryFormat
						for x in self._output_io
					]
					self._output_io = tuple([
						x or typio.decode for x in self._output_io
					])

	def __next__(self):
		result = self._fetch(self.direction, 1)
		if not result:
			raise StopIteration
		else:
			return result[0]

	def read(self, quantity = None, direction = None):
		if quantity == 0:
			return []
		dir = self._which_way(direction)
		return self._fetch(dir, quantity)

	def _fetch(self, direction, quantity):
		x = self._ins(
			self._pq_xp_fetch(direction, quantity) + \
			(element.SynchronizeMessage,)
		)
		self.database._pq_push(x, self)
		self.database._pq_complete()
		return self._process_tuple((
			y for y in x.messages_received() if y.__class__ is tuple
		))

	def seek(self, offset, whence = 'ABSOLUTE'):
		rwhence = self._seek_whence_map.get(whence, whence)
		if rwhence is None or rwhence.upper() not in \
		self._seek_whence_map.values():
			raise TypeError(
				"unknown whence parameter, %r" %(whence,)
			)
		rwhence = rwhence.upper()

		if offset == 'ALL':
			if rwhence not in ('BACKWARD', 'FORWARD'):
				rwhence = 'BACKWARD' if self.direction is False else 'FORWARD'
		else:
			if offset < 0 and rwhence == 'BACKWARD':
				offset = -offset
				rwhence = 'FORWARD'

			if self.direction is False:
				if offset == 'ALL' and rwhence != 'FORWARD':
					rwhence = 'BACKWARD'
				else:
					if rwhence == 'RELATIVE':
						offset = -offset
					elif rwhence == 'ABSOLUTE':
						rwhence = 'FROM_END'
					else:
						rwhence = 'ABSOLUTE'

		if rwhence in ('RELATIVE', 'BACKWARD', 'FORWARD'):
			if offset == 'ALL':
				cmd = self._pq_xp_move(
					str(offset).encode('ascii'), str(rwhence).encode('ascii')
				)
			else:
				if offset < 0:
					cmd = self._pq_xp_move(
						str(-offset).encode('ascii'), b'BACKWARD'
					)
				else:
					cmd = self._pq_xp_move(
						str(offset).encode('ascii'), str(rwhence).encode('ascii')
					)
		elif rwhence == 'ABSOLUTE':
			cmd = self._pq_xp_move(str(offset).encode('ascii'), b'ABSOLUTE')
		else:
			# move to last record, then consume it to put the position at
			# the very end of the cursor.
			cmd = self._pq_xp_move(b'', b'LAST') + \
				self._pq_xp_move(b'', b'NEXT') + \
				self._pq_xp_move(str(offset).encode('ascii'), b'BACKWARD')

		x = self._ins(cmd + (element.SynchronizeMessage,),)
		self.database._pq_push(x, self)
		self.database._pq_complete()

		count = None
		complete = element.Complete.type
		for cm in x.messages_received():
			if getattr(cm, 'type', None) == complete:
				count = cm.extract_count()
				break

		# XXX: Raise if count is None?
		return count

class SingleExecution(pg_api.Execution):
	database = None
	def __init__(self, database):
		self._prepare = database.prepare

	def load_rows(self, query, *parameters):
		return self._prepare(query).load_rows(*parameters)

	def load_chunks(self, query, *parameters):
		return self._prepare(query).load_chunks(*parameters)

	def __call__(self, query, *parameters):
		return self._prepare(query)(*parameters)

	def declare(self, query, *parameters):
		return self._prepare(query).declare(*parameters)

	def rows(self, query, *parameters):
		return self._prepare(query).rows(*parameters)

	def chunks(self, query, *parameters):
		return self._prepare(query).chunks(*parameters)

	def column(self, query, *parameters):
		return self._prepare(query).column(*parameters)

	def first(self, query, *parameters):
		return self._prepare(query).first(*parameters)

class Statement(pg_api.Statement):
	string = None
	database = None
	statement_id = None
	_input = None
	_output = None
	_output_io = None
	_output_formats = None
	_output_attmap = None

	def _e_metas(self):
		yield (None, '[' + self.state + ']')
		if hasattr(self._xact, 'error_message'):
			# be very careful not to trigger an exception.
			# even in the cases of effective protocol errors,
			# it is important not to bomb out.
			pos = self._xact.error_message.get(b'P')
			if pos is not None and pos.isdigit():
				try:
					pos = int(pos)
					# get the statement source
					q = str(self.string)
					# normalize position..
					pos = len('\n'.join(q[:pos].splitlines()))
					# normalize newlines
					q = '\n'.join(q.splitlines())
					line_no = q.count('\n', 0, pos) + 1
					# replace tabs with spaces because there is no way to identify
					# the tab size of the final display. (ie, marker will be wrong)
					q = q.replace('\t', ' ')
					# grab the relevant part of the query string.
					# the full source will be printed elsewhere.
					# beginning of string or the newline before the position
					bov = q.rfind('\n', 0, pos) + 1
					# end of string or the newline after the position
					eov = q.find('\n', pos)
					if eov == -1:
						eov = len(q)
					view = q[bov:eov]
					# position relative to the beginning of the view
					pos = pos-bov
					# analyze lines prior to position
					dlines = view.splitlines()
					marker = ((pos-1) * ' ') + '^' + (
						' [line %d, character %d] ' %(line_no, pos)
					)
					# insert marker
					dlines.append(marker)
					yield ('LINE', os.linesep.join(dlines))
				except:
					import traceback
					yield ('LINE', traceback.format_exc(chain=False))
		spt = self.sql_parameter_types
		if spt is not None:
			yield ('sql_parameter_types', spt)
		cn = self.column_names
		ct = self.sql_column_types
		if cn is not None:
			if ct is not None:
				yield (
					'results',
					'(' + ', '.join([
						'{!r} {!r}'.format(n, t) for n,t in zip(cn,ct)
					]) + ')'
				)
			else:
				yield ('sql_column_names', cn)
		elif ct is not None:
			yield ('sql_column_types', ct)

	def clone(self):
		ps = self.__class__(self.database, None, self.string)
		ps._init()
		ps._fini()
		return ps

	def __init__(self,
		database, statement_id, string,
		wref = weakref.ref
	):
		self.database = database
		self.string = string
		self.statement_id = statement_id or ID(self)
		self._xact = None
		self.closed = None
		self._pq_statement_id = database.typio._encode(self.statement_id)[0]

		if not statement_id:
			# Register statement on a connection to close it automatically on db end
			database.pq.register_statement(self, self._pq_statement_id)

	def __repr__(self):
		return '<{mod}.{name}[{ci}] {state}>'.format(
			mod = self.__class__.__module__,
			name = self.__class__.__name__,
			ci = self.database.connector._pq_iri,
			state = self.state,
		)

	def _pq_parameters(self, parameters, proc = process_tuple):
		return proc(
			self._input_io, parameters,
			self._raise_parameter_tuple_error
		)

	##
	# process_tuple failed(exception). The parameters could not be packed.
	# This function is called with the given information in the context
	# of the original exception(to allow chaining).
	def _raise_parameter_tuple_error(self, cause, procs, tup, itemnum):
		# Find the SQL type name. This should *not* hit the server.
		typ = self.database.typio.sql_type_from_oid(
			self.pg_parameter_types[itemnum]
		) or '<unknown>'

		# Representation of the bad parameter.
		bad_data = repr(tup[itemnum])
		if len(bad_data) > 80:
			# Be sure not to fill screen with noise.
			bad_data = bad_data[:75] + ' ...'

		em = element.ClientError((
			(b'S', 'ERROR'),
			(b'C', '--PIO'),
			(b'M', "could not pack parameter %s::%s for transfer" %(
					('$' + str(itemnum + 1)), typ,
				)
			),
			(b'D', bad_data),
			(b'H', "Try casting the parameter to 'text', then to the target type."),
			(b'P', str(itemnum))
		))
		self.database.typio.raise_client_error(em, creator = self, cause = cause)

	##
	# Similar to the parameter variant.
	def _raise_column_tuple_error(self, cause, procs, tup, itemnum):
		# Find the SQL type name. This should *not* hit the server.
		typ = self.database.typio.sql_type_from_oid(
			self.pg_column_types[itemnum]
		) or '<unknown>'

		# Representation of the bad column.
		data = repr(tup[itemnum])
		if len(data) > 80:
			# Be sure not to fill screen with noise.
			data = data[:75] + ' ...'

		em = element.ClientError((
			(b'S', 'ERROR'),
			(b'C', '--CIO'),
			(b'M', "could not unpack column %r, %s::%s, from wire data" %(
					itemnum, self.column_names[itemnum], typ
				)
			),
			(b'D', data),
			(b'H', "Try casting the column to 'text'."),
			(b'P', str(itemnum)),
		))
		self.database.typio.raise_client_error(em, creator = self, cause = cause)

	@property
	def state(self) -> str:
		if self.closed:
			if self._xact is not None:
				if self.string is not None:
					return 'parsing'
				else:
					return 'describing'
			return 'closed'
		return 'prepared'

	@property
	def column_names(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return list(self.database.typio.decodes(self._output.keys()))

	@property
	def parameter_types(self):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			return [self.database.typio.type_from_oid(x) for x in self._input]

	@property
	def column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [
				self.database.typio.type_from_oid(x[3]) for x in self._output
			]

	@property
	def pg_parameter_types(self):
		if self.closed is None:
			self._fini()
		return self._input

	@property
	def pg_column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [x[3] for x in self._output]

	@property
	def sql_column_types(self):
		if self.closed is None:
			self._fini()
		if self._output is not None:
			return [
				self.database.typio.sql_type_from_oid(x)
				for x in self.pg_column_types
			]

	@property
	def sql_parameter_types(self):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			return [
				self.database.typio.sql_type_from_oid(x)
				for x in self.pg_parameter_types
			]

	def close(self):
		if self.closed is False:
			self.database.pq.trash_statement(self._pq_statement_id)
		self.closed = True

	def _init(self):
		"""
		Push initialization messages to the server, but don't wait for
		the return as there may be things that can be done while waiting
		for the return. Use the _fini() to complete.
		"""
		if self.string is not None:
			q = self.database.typio._encode(str(self.string))[0]
			cmd = [
				element.CloseStatement(self._pq_statement_id),
				element.Parse(self._pq_statement_id, q, ()),
			]
		else:
			cmd = []
		cmd.extend(
			(
				element.DescribeStatement(self._pq_statement_id),
				element.SynchronizeMessage,
			)
		)
		self._xact = xact.Instruction(cmd, asynchook = self.database._receive_async)
		self.database._pq_push(self._xact, self)

	def _fini(self, strfmt = element.StringFormat, binfmt = element.BinaryFormat):
		"""
		Complete initialization that the _init() method started.
		"""
		# assume that the transaction has been primed.
		if self._xact is None:
			raise RuntimeError("_fini called prior to _init; invalid state")
		if self._xact is self.database.pq.xact:
			try:
				self.database._pq_complete()
			except Exception:
				self.closed = True
				raise

		(*head, argtypes, tupdesc, last) = self._xact.messages_received()

		typio = self.database.typio
		if tupdesc is None or tupdesc is element.NoDataMessage:
			# Not typed output.
			self._output = None
			self._output_attmap = None
			self._output_io = None
			self._output_formats = None
			self._row_constructor = None
		else:
			self._output = tupdesc
			self._output_attmap = dict(
				typio.attribute_map(tupdesc)
			)
			self._row_constructor = self.database.typio.RowTypeFactory(self._output_attmap)
			# tuple output
			self._output_io = typio.resolve_descriptor(tupdesc, 1)
			self._output_formats = [
				strfmt if x is None else binfmt
				for x in self._output_io
			]
			self._output_io = tuple([
				x or typio.decode for x in self._output_io
			])

		self._input = argtypes
		packs = []
		formats = []
		for x in argtypes:
			pack = (typio.resolve(x) or (None,None))[0]
			packs.append(pack or typio.encode)
			formats.append(
				strfmt if x is None else binfmt
			)
		self._input_io = tuple(packs)
		self._input_formats = formats
		self.closed = False
		self._xact = None

	def __call__(self, *parameters):
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		##
		# get em' all!
		if self._output is None:
			# might be a copy.
			c = SingleXactCopy(self, parameters)
		else:
			c = SingleXactFetch(self, parameters)
			c._process_chunk = c._process_tuple_chunk_Row

		# iff output is None, it's not a tuple returning query.
		# however, if it's a copy, detect that fact by SingleXactCopy's
		# immediate return after finding the copy begin message(no complete).
		if self._output is None:
			cmd = c.command()
			if cmd is not None:
				return (cmd, c.count())
		# Returns rows, accumulate in a list.
		r = []
		for x in c:
			r.extend(x)
		return r

	def declare(self, *parameters):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))
		return Cursor(self, parameters, self.database, None)

	def rows(self, *parameters, **kw):
		chunks = self.chunks(*parameters, **kw)
		if chunks._output_io:
			chunks._process_chunk = chunks._process_tuple_chunk_Row
		return chain.from_iterable(chunks)
	__iter__ = rows

	def chunks(self, *parameters):
		if self.closed is None:
			self._fini()
		if self._input is not None:
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))

		if self._output is None:
			# It's *probably* a COPY.
			return SingleXactCopy(self, parameters)
		if self.database.pq.state == b'I':
			# Currently, *not* in a Transaction block, so
			# DECLARE the statement WITH HOLD in order to allow
			# access across transactions.
			if self.string is not None:
				return MultiXactOutsideBlock(self, parameters, None)
			else:
				##
				# Statement source unknown, so it can't be DECLARE'd.
				# This happens when statement_from_id is used.
				return SingleXactFetch(self, parameters)
		else:
			# Likely, the best possible case. It gets to use Execute messages.
			return MultiXactInsideBlock(self, parameters, None)

	def column(self, *parameters, **kw):
		chunks = self.chunks(*parameters, **kw)
		chunks._process_chunk = chunks._process_tuple_chunk_Column
		return chain.from_iterable(chunks)

	def first(self, *parameters):
		if self.closed is None:
			# Not fully initialized; assume interrupted.
			self._fini()
		if self._input is not None:
			# Use a regular TypeError.
			if len(parameters) != len(self._input):
				raise TypeError("statement requires %d parameters, given %d" %(
					len(self._input), len(parameters)
				))

		# Parameters? Build em'.
		db = self.database

		if self._input_io:
			params = process_tuple(
				self._input_io, parameters,
				self._raise_parameter_tuple_error
			)
		else:
			params = ()

		# Run the statement
		x = xact.Instruction((
				element.Bind(
					b'',
					self._pq_statement_id,
					self._input_formats,
					params,
					self._output_formats or (),
				),
				# Get all
				element.Execute(b'', 0xFFFFFFFF),
				element.ClosePortal(b''),
				element.SynchronizeMessage
			),
			asynchook = db._receive_async
		)
		# Push and complete protocol transaction.
		db._pq_push(x, self)
		db._pq_complete()

		if self._output_io:
			##
			# It returned rows, look for the first tuple.
			tuple_type = element.Tuple.type
			for xt in x.messages_received():
				if xt.__class__ is tuple:
					break
			else:
				return None

			if len(self._output_io) > 1:
				# Multiple columns, return a Row.
				return self._row_constructor(
					process_tuple(
						self._output_io, xt,
						self._raise_column_tuple_error
					)
				)
			else:
				# Single column output.
				if xt[0] is None:
					return None
				io = self._output_io[0] or self.database.typio.decode
				return io(xt[0])
		else:
			##
			# It doesn't return rows, so return a count.
			##
			# This loop searches through the received messages
			# for the Complete message which contains the count.
			complete = element.Complete.type
			for cm in x.messages_received():
				# Use getattr because COPY doesn't produce
				# element.Message instances.
				if getattr(cm, 'type', None) == complete:
					break
			else:
				# Probably a Null command.
				return None

			count = cm.extract_count()
			if count is None:
				command = cm.extract_command()
				if command is not None:
					return command.decode('ascii')
			return count

	def _load_copy_chunks(self, chunks, *parameters):
		"""
		Given an chunks of COPY lines, execute the COPY ... FROM STDIN
		statement and send the copy lines produced by the iterable to
		the remote end.
		"""
		x = xact.Instruction((
				element.Bind(
					b'',
					self._pq_statement_id,
					(), (), (),
				),
				element.Execute(b'', 1),
				element.SynchronizeMessage,
			),
			asynchook = self.database._receive_async
		)
		self.database._pq_push(x, self)

		# localize
		step = self.database._pq_step

		# Get the COPY started.
		while x.state is not xact.Complete:
			step()
			if hasattr(x, 'CopyFailSequence') and x.messages is x.CopyFailSequence:
				# The protocol transaction has noticed that its a COPY.
				break
		else:
			# Oh, it's not a COPY at all.
			x.fatal = x.fatal or False
			x.error_message = element.ClientError((
				(b'S', 'ERROR'),
				# OperationError
				(b'C', '--OPE'),
				(b'M', "_load_copy_chunks() used on a non-COPY FROM STDIN query"),
			))
			self.database.typio.raise_client_error(x.error_message, creator = self)

		for chunk in chunks:
			x.messages = list(chunk)
			while x.messages is not x.CopyFailSequence:
				# Continue stepping until the transaction
				# sets the CopyFailSequence again. That's
				# the signal that the transaction has sent
				# all the previously set messages.
				step()
		x.messages = x.CopyDoneSequence
		self.database._pq_complete()
		self.database.pq.synchronize()

	def _load_tuple_chunks(self, chunks):
		pte = self._raise_parameter_tuple_error
		last = (element.SynchronizeMessage,)
		try:
			for chunk in chunks:
				bindings = [
					(
						element.Bind(
							b'',
							self._pq_statement_id,
							self._input_formats,
							process_tuple(
								self._input_io, tuple(t), pte
							),
							(),
						),
						element.Execute(b'', 1),
					)
					for t in chunk
				]
				bindings.append(last)
				self.database._pq_push(
					xact.Instruction(
						chain.from_iterable(bindings),
						asynchook = self.database._receive_async
					),
					self
				)
			self.database._pq_complete()
		except:
			##
			# In cases where row packing errors or occur,
			# synchronize, finishing any pending transaction,
			# and raise the error.
			##
			# If the data sent to the remote end is invalid,
			# _complete will raise the exception and the current
			# exception being marked as the cause, so there should
			# be no [exception] information loss.
			##
			self.database.pq.synchronize()
			raise

	def load_chunks(self, chunks, *parameters):
		"""
		Execute the query for each row-parameter set in `iterable`.

		In cases of ``COPY ... FROM STDIN``, iterable must be an iterable of
		sequences of `bytes`.
		"""
		if self.closed is None:
			self._fini()
		if not self._input or parameters:
			return self._load_copy_chunks(chunks)
		else:
			return self._load_tuple_chunks(chunks)

	def load_rows(self, rows, chunksize = 256):
		return self.load_chunks(chunk(rows, chunksize))
PreparedStatement = Statement

class StoredProcedure(pg_api.StoredProcedure):
	_e_factors = ('database', 'procedure_id')
	procedure_id = None

	def _e_metas(self):
		yield ('oid', self.oid)

	def __repr__(self):
		return '<%s:%s>' %(
			self.procedure_id, self.statement.string
		)

	def __call__(self, *args, **kw):
		if kw:
			input = []
			argiter = iter(args)
			try:
				word_idx = [(kw[k], self._input_attmap[k]) for k in kw]
			except KeyError as k:
				raise TypeError("%s got unexpected keyword argument %r" %(
						self.name, k.message
					)
				)
			word_idx.sort(key = get1)
			current_word = word_idx.pop(0)
			for x in range(argc):
				if x == current_word[1]:
					input.append(current_word[0])
					current_word = word_idx.pop(0)
				else:
					input.append(argiter.next())
		else:
			input = args

		if self.srf is True:
			if self.composite is True:
				return self.statement.rows(*input)
			else:
				# A generator expression is very appropriate here
				# as SRFs returning large number of rows would require
				# substantial amounts of memory.
				return map(get0, self.statement.rows(*input))
		else:
			if self.composite is True:
				return self.statement(*input)[0]
			else:
				return self.statement(*input)[0][0]

	def __init__(self, ident, database, description = ()):
		# Lookup pg_proc on database.
		if isinstance(ident, int):
			proctup = database.sys.lookup_procedure_oid(int(ident))
		else:
			proctup = database.sys.lookup_procedure_rp(str(ident))
		if proctup is None:
			raise LookupError("no function with identifier %s" %(str(ident),))

		self.procedure_id = ident
		self.oid = proctup[0]
		self.name = proctup["proname"]

		self._input_attmap = {}
		argnames = proctup.get('proargnames') or ()
		for x in range(len(argnames)):
			an = argnames[x]
			if an is not None:
				self._input_attmap[an] = x

		proargs = proctup['proargtypes']
		for x in proargs:
			# get metadata filled out.
			database.typio.resolve(x)

		self.statement = database.prepare(
			"SELECT * FROM %s(%s) AS func%s" %(
				proctup['_proid'],
				# ($1::type, $2::type, ... $n::type)
				', '.join([
					 '$%d::%s' %(x + 1, database.typio.sql_type_from_oid(proargs[x]))
					 for x in range(len(proargs))
				]),
				# Description for anonymous record returns
				(description and \
					'(' + ','.join(description) + ')' or '')
			)
		)
		self.srf = bool(proctup.get("proretset"))
		self.composite = proctup["composite"]

class SettingsCM(object):
	def __init__(self, database, settings_to_set):
		self.database = database
		self.settings_to_set = settings_to_set

	def __enter__(self):
		if hasattr(self, 'stored_settings'):
			raise RuntimeError("cannot re-use setting CMs")
		self.stored_settings = self.database.settings.getset(
			self.settings_to_set.keys()
		)
		self.database.settings.update(self.settings_to_set)

	def __exit__(self, typ, val, tb):
		self.database.settings.update(self.stored_settings)

class Settings(pg_api.Settings):
	_e_factors = ('database',)

	def __init__(self, database):
		self.database = database
		self.cache = {}

	def _e_metas(self):
		yield (None, str(len(self.cache)))

	def _clear_cache(self):
		self.cache.clear()

	def __getitem__(self, i):
		v = self.cache.get(i)
		if v is None:
			r = self.database.sys.setting_get(i)

			if r:
				v = r[0][0]
			else:
				raise KeyError(i)
		return v

	def __setitem__(self, i, v):
		cv = self.cache.get(i)
		if cv == v:
			return
		setas = self.database.sys.setting_set(i, v)
		self.cache[i] = setas

	def __delitem__(self, k):
		self.database.execute(
			'RESET "' + k.replace('"', '""') + '"'
		)
		self.cache.pop(k, None)

	def __len__(self):
		return self.database.sys.setting_len()

	def __call__(self, **settings):
		return SettingsCM(self.database, settings)

	def path():
		def fget(self):
			return pg_str.split_ident(self["search_path"])
		def fset(self, value):
			self['search_path'] = ','.join([
				'"%s"' %(x.replace('"', '""'),) for x in value
			])
		def fdel(self):
			if self.database.connector.path is not None:
				self.path = self.database.connector.path
			else:
				self.database.execute("RESET search_path")
		doc = 'structured search_path interface'
		return locals()
	path = property(**path())

	def get(self, k, alt = None):
		if k in self.cache:
			return self.cache[k]

		db = self.database
		r = self.database.sys.setting_get(k)
		if r:
			v = r[0][0]
			self.cache[k] = v
		else:
			v = alt
		return v

	def getset(self, keys):
		setmap = {}
		rkeys = []
		for k in keys:
			v = self.cache.get(k)
			if v is not None:
				setmap[k] = v
			else:
				rkeys.append(k)

		if rkeys:
			r = self.database.sys.setting_mget(rkeys)
			self.cache.update(r)
			setmap.update(r)
			rem = set(rkeys) - set([x['name'] for x in r])
			if rem:
				raise KeyError(rem)
		return setmap

	def keys(self):
		return map(get0, self.database.sys.setting_keys())
	__iter__ = keys

	def values(self):
		return map(get0, self.database.sys.setting_values())

	def items(self):
		return self.database.sys.setting_items()

	def update(self, d):
		kvl = [list(x) for x in dict(d).items()]
		self.cache.update(self.database.sys.setting_update(kvl))

	def _notify(self, msg):
		subs = getattr(self, '_subscriptions', {})
		d = self.database.typio._decode
		key = d(msg.name)[0]
		val = d(msg.value)[0]
		for x in subs.get(key, ()):
			x(self.database, key, val)
		if None in subs:
			for x in subs[None]:
				x(self.database, key, val)
		self.cache[key] = val

	def subscribe(self, key, callback):
		"""
		Subscribe to changes of the setting using the callback. When the setting
		is changed, the callback will be invoked with the connection, the key,
		and the new value. If the old value is locally cached, its value will
		still be available for inspection, but there is no guarantee.
		If `None` is passed as the key, the callback will be called whenever any
		setting is remotely changed.

		>>> def watch(connection, key, newval):
		...
		>>> db.settings.subscribe('TimeZone', watch)
		"""
		subs = self._subscriptions = getattr(self, '_subscriptions', {})
		callbacks = subs.setdefault(key, [])
		if callback not in callbacks:
			callbacks.append(callback)

	def unsubscribe(self, key, callback):
		"""
		Stop listening for changes to a setting. The setting name(`key`), and
		the callback used to subscribe must be given again for successful
		termination of the subscription.

		>>> db.settings.unsubscribe('TimeZone', watch)
		"""
		subs = getattr(self, '_subscriptions', {})
		callbacks = subs.get(key, ())
		if callback in callbacks:
			callbacks.remove(callback)

class Transaction(pg_api.Transaction):
	database = None

	mode = None
	isolation = None

	_e_factors = ('database', 'isolation', 'mode')

	def _e_metas(self):
		yield (None, self.state)

	def __init__(self, database, isolation = None, mode = None):
		self.database = database
		self.isolation = isolation
		self.mode = mode
		self.state = 'initialized'
		self.type = None

	def __enter__(self):
		self.start()
		return self

	def __exit__(self, typ, value, tb):
		if typ is None:
			# No exception, but in a failed transaction?
			if self.database.pq.state == b'E':
				if not self.database.closed:
					self.rollback()
				# pg_exc.InFailedTransactionError
				em = element.ClientError((
					(b'S', 'ERROR'),
					(b'C', '25P02'),
					(b'M', 'invalid transaction block exit detected'),
					(b'H', "Database was in an error-state, but no exception was raised.")
				))
				self.database.typio.raise_client_error(em, creator = self)
			else:
				# No exception, and no error state. Everything is good.
				try:
					self.commit()
					# If an error occurs, clean up the transaction state
					# and raise as needed.
				except pg_exc.ActiveTransactionError as err:
					if not self.database.closed:
						# adjust the state so rollback will do the right thing and abort.
						self.state = 'open'
						self.rollback()
					raise
		elif issubclass(typ, Exception):
			# There's an exception, so only rollback if the connection
			# exists. If the rollback() was called here, it would just
			# contribute noise to the error.
			if not self.database.closed:
				self.rollback()

	@staticmethod
	def _start_xact_string(isolation = None, mode = None):
		q = 'START TRANSACTION'
		if isolation is not None:
			if ';' in isolation:
				raise ValueError("invalid transaction isolation " + repr(mode))
			q += ' ISOLATION LEVEL ' + isolation
		if mode is not None:
			if ';' in mode:
				raise ValueError("invalid transaction mode " + repr(isolation))
			q += ' ' + mode
		return q + ';'

	@staticmethod
	def _savepoint_xact_string(id):
		return 'SAVEPOINT "xact(' + id.replace('"', '""') + ')";'

	def start(self):
		if self.state == 'open':
			return
		if self.state != 'initialized':
			em = element.ClientError((
				(b'S', 'ERROR'),
				(b'C', '--OPE'),
				(b'M', "transactions cannot be restarted"),
				(b'H', 'Create a new transaction object instead of re-using an old one.')
			))
			self.database.typio.raise_client_error(em, creator = self)

		if self.database.pq.state == b'I':
			self.type = 'block'
			q = self._start_xact_string(
				isolation = self.isolation,
				mode = self.mode,
			)
		else:
			self.type = 'savepoint'
			if (self.isolation, self.mode) != (None,None):
				em = element.ClientError((
					(b'S', 'ERROR'),
					(b'C', '--OPE'),
					(b'M', "configured transaction used inside a transaction block"),
					(b'H', 'A transaction block was already started.'),
				))
				self.database.typio.raise_client_error(em, creator = self)
			q = self._savepoint_xact_string(hex(id(self)))
		self.database.execute(q)
		self.state = 'open'
	begin = start

	@staticmethod
	def _release_string(id):
		'release "";'
		return 'RELEASE "xact(' + id.replace('"', '""') + ')";'

	def commit(self):
		if self.state == 'committed':
			return
		if self.state != 'open':
			em = element.ClientError((
				(b'S', 'ERROR'),
				(b'C', '--OPE'),
				(b'M', "commit attempted on transaction with unexpected state, " + repr(self.state)),
			))
			self.database.typio.raise_client_error(em, creator = self)

		if self.type == 'block':
			q = 'COMMIT'
		else:
			q = self._release_string(hex(id(self)))
		self.database.execute(q)
		self.state = 'committed'

	@staticmethod
	def _rollback_to_string(id, fmt = 'ROLLBACK TO "xact({0})"; RELEASE "xact({0})";'.format):
		return fmt(id.replace('"', '""'))

	def rollback(self):
		if self.state == 'aborted':
			return
		if self.state not in ('prepared', 'open'):
			em = element.ClientError((
				(b'S', 'ERROR'),
				(b'C', '--OPE'),
				(b'M', "ABORT attempted on transaction with unexpected state, " + repr(self.state)),
			))
			self.database.typio.raise_client_error(em, creator = self)

		if self.type == 'block':
			q = 'ABORT;'
		elif self.type == 'savepoint':
			q = self._rollback_to_string(hex(id(self)))
		else:
			raise RuntimeError("unknown transaction type " + repr(self.type))
		self.database.execute(q)
		self.state = 'aborted'
	abort = rollback

class Connection(pg_api.Connection):
	connector = None

	type = None
	version_info = None
	version = None

	security = None
	backend_id = None
	client_address = None
	client_port = None

	# Replaced with instances on connection instantiation.
	settings = Settings

	def _e_metas(self):
		yield (None, '[' + self.state + ']')
		if self.client_address is not None:
			yield ('client_address', self.client_address)
		if self.client_port is not None:
			yield ('client_port', self.client_port)
		if self.version is not None:
			yield ('version', self.version)
		att = getattr(self, 'failures', None)
		if att:
			count = 0
			for x in att:
				# Format each failure without their traceback.
				errstr = ''.join(format_exception(type(x.error), x.error, None))
				factinfo = str(x.socket_factory)
				if hasattr(x, 'ssl_negotiation'):
					if x.ssl_negotiation is True:
						factinfo = 'SSL ' + factinfo
					else:
						factinfo = 'NOSSL ' + factinfo
				yield (
					'failures[' + str(count) + ']',
					factinfo + os.linesep + errstr
				)
				count += 1

	def __repr__(self):
		return '<%s.%s[%s] %s>' %(
			type(self).__module__,
			type(self).__name__,
			self.connector._pq_iri,
			self.closed and 'closed' or '%s' %(self.pq.state,)
		)

	def __exit__(self, type, value, tb):
		# Don't bother closing unless it's a normal exception.
		if type is None or issubclass(type, Exception):
			self.close()

	def interrupt(self, timeout = None):
		self.pq.interrupt(timeout = timeout)

	def execute(self, query : str) -> None:
		q = xact.Instruction((
				element.Query(self.typio._encode(query)[0]),
			),
			asynchook = self._receive_async
		)
		self._pq_push(q, self)
		self._pq_complete()

	def do(self, language : str, source : str,
		qlit = pg_str.quote_literal,
		qid = pg_str.quote_ident,
	) -> None:
		sql = "DO " + qlit(source) + " LANGUAGE " + qid(language) + ";"
		self.execute(sql)

	def xact(self, isolation = None, mode = None):
		x = Transaction(self, isolation = isolation, mode = mode)
		return x

	def prepare(self,
		sql_statement_string : str,
		statement_id = None,
		Class = Statement
	) -> Statement:
		ps = Class(self, statement_id, sql_statement_string)
		ps._init()
		ps._fini()
		return ps

	@property
	def query(self, Class = SingleExecution):
		return Class(self)

	def statement_from_id(self, statement_id : str) -> Statement:
		ps = Statement(self, statement_id, None)
		ps._init()
		ps._fini()
		return ps

	def proc(self, proc_id : (str, int)) -> StoredProcedure:
		sp = StoredProcedure(proc_id, self)
		return sp

	def cursor_from_id(self, cursor_id : str) -> Cursor:
		c = Cursor(None, None, self, cursor_id)
		c._init()
		return c

	@property
	def closed(self) -> bool:
		if getattr(self, 'pq', None) is None:
			return True
		if hasattr(self.pq, 'socket') and self.pq.xact is not None:
			return self.pq.xact.fatal is True
		return False

	def close(self, getattr = getattr):
		# Write out the disconnect message if the socket is around.
		# If the connection is known to be lost, don't bother. It will
		# generate an extra exception.
		if getattr(self, 'pq', None) is None or getattr(self.pq, 'socket', None) is None:
			# No action to take.
			return

		x = getattr(self.pq, 'xact', None)
		if x is not None and x.fatal is not True:
			# finish the existing pq transaction iff it's not Closing.
			self.pq.complete()

		if self.pq.xact is None:
			# It completed the existing transaction.
			self.pq.push(xact.Closing())
			self.pq.complete()
			if self.pq.socket:
				self.pq.complete()

		# Close the socket if there is one.
		if self.pq.socket:
			self.pq.socket.close()
			self.pq.socket = None

	@property
	def state(self) -> str:
		if not hasattr(self, 'pq'):
			return 'initialized'
		if hasattr(self, 'failures'):
			return 'failed'
		if self.closed:
			return 'closed'
		if isinstance(self.pq.xact, xact.Negotiation):
			return 'negotiating'
		if self.pq.xact is None:
			if self.pq.state == b'E':
				return 'failed block'
			return 'idle' + (' in block' if self.pq.state != b'I' else '')
		else:
			return 'busy'

	def reset(self):
		"""
		restore original settings, reset the transaction, drop temporary
		objects.
		"""
		self.execute("ABORT; RESET ALL;")

	def __enter__(self):
		self.connect()
		return self

	def connect(self):
		'Establish the connection to the server'
		if self.closed is False:
			# already connected? just return.
			return

		if hasattr(self, 'pq'):
			# It's closed, *but* there's a PQ connection..
			x = self.pq.xact
			self.typio.raise_error(x.error_message, cause = getattr(x, 'exception', None), creator = self)

		# It's closed.
		try:
			self._establish()
		except Exception:
			# Close it up on failure.
			self.close()
			raise

	def _establish(self):
		# guts of connect()
		self.pq = None
		# if any exception occurs past this point, the connection
		# will not be usable.
		timeout = self.connector.connect_timeout
		sslmode = self.connector.sslmode or 'prefer'
		failures = []
		exc = None
		try:
			# get the list of sockets to try
			socket_factories = self.connector.socket_factory_sequence()
		except Exception as e:
			socket_factories = ()
			exc = e

		# When ssl is None: SSL negotiation will not occur.
		# When ssl is True: SSL negotiation will occur *and* it must succeed.
		# When ssl is False: SSL negotiation will occur but it may fail(NOSSL).
		if sslmode == 'allow':
			# without ssl, then with. :)
			socket_factories = interlace(
				zip(repeat(None, len(socket_factories)), socket_factories),
				zip(repeat(True, len(socket_factories)), socket_factories)
			)
		elif sslmode == 'prefer':
			# with ssl, then without. [maybe] :)
			socket_factories = interlace(
				zip(repeat(False, len(socket_factories)), socket_factories),
				zip(repeat(None, len(socket_factories)), socket_factories)
			)
			# prefer is special, because it *may* be possible to
			# skip the subsequent "without" in situations where SSL is off.
		elif sslmode == 'require':
			socket_factories = zip(repeat(True, len(socket_factories)), socket_factories)
		elif sslmode == 'disable':
			# None = Do Not Attempt SSL negotiation.
			socket_factories = zip(repeat(None, len(socket_factories)), socket_factories)
		else:
			raise ValueError("invalid sslmode: " + repr(sslmode))

		# can_skip is used when 'prefer' or 'allow' is the sslmode.
		# if the ssl negotiation returns 'N' (nossl), then
		# ssl "failed", but the socket is still usable for nossl.
		# in these cases, can_skip is set to True so that the
		# subsequent non-ssl attempt is skipped if it failed with the 'N' response.
		can_skip = False
		startup = self.connector._startup_parameters
		password = self.connector._password
		Connection3 = client.Connection
		for (ssl, sf) in socket_factories:
			if can_skip is True:
				# the last attempt failed and knows this attempt will fail too.
				can_skip = False
				continue
			pq = Connection3(sf, startup, password = password,)
			if hasattr(self, 'tracer'):
				pq.tracer = self.tracer

			# Grab the negotiation transaction before
			# connecting as it will be needed later if successful.
			neg = pq.xact
			pq.connect(ssl = ssl, timeout = timeout)

			didssl = getattr(pq, 'ssl_negotiation', -1)

			# It successfully connected if pq.xact is None;
			# The startup/negotiation xact completed.
			if pq.xact is None:
				self.pq = pq
				if hasattr(self.pq.socket, 'fileno'):
					self.fileno = self.pq.socket.fileno
				self.security = 'ssl' if didssl is True else None
				showoption_type = element.ShowOption.type
				for x in neg.asyncs:
					if x.type == showoption_type:
						self._receive_async(x)
				# success!
				break
			elif pq.socket is not None:
				# In this case, an application/protocol error occurred.
				# Close out the sockets ourselves.
				pq.socket.close()

			# Identify whether or not we can skip the attempt.
			# Whether or not we can skip depends entirely on the SSL parameter.
			if sslmode == 'prefer' and ssl is False and didssl is False:
				# In this case, the server doesn't support SSL or it's
				# turned off. Therefore, the "without_ssl" attempt need
				# *not* be ran because it has already been noted to be
				# a failure.
				can_skip = True
			elif hasattr(pq.xact, 'exception'):
				# If a Python exception occurred, chances are that it is
				# going to fail again iff it is going to hit the same host.
				if sslmode == 'prefer' and ssl is False:
					# when 'prefer', the first attempt
					# is marked with ssl is "False"
					can_skip = True
				elif sslmode == 'allow' and ssl is None:
					# when 'allow', the first attempt
					# is marked with dossl is "None"
					can_skip = True

			try:
				self.typio.raise_error(pq.xact.error_message)
			except Exception as error:
				pq.error = error
				# Otherwise, infinite recursion in the element traceback.
				error.creator = None
				# The tracebacks of the specific failures aren't particularly useful..
				error.__traceback__ = None
			if getattr(pq.xact, 'exception', None) is not None:
				pq.error.__cause__ = pq.xact.exception

			failures.append(pq)
		else:
			# No servers available. (see the break-statement in the for-loop)
			self.failures = failures or ()
			# it's over.
			self.typio.raise_client_error(could_not_connect, creator = self, cause = exc)
		##
		# connected, now initialize connection information.
		self.backend_id = self.pq.backend_id

		sv = self.settings.cache.get("server_version", "0.0")
		self.version_info = pg_version.normalize(pg_version.split(sv))
		# manual binding
		self.sys = pg_lib.Binding(self, pg_lib.sys)

		vi = self.version_info[:2]
		if vi <= (8,1):
			sd = self.sys.startup_data_only_version()
		elif vi >= (9,2):
			sd = self.sys.startup_data_92()
		else:
			sd = self.sys.startup_data()
		# connection info
		self.version, self.backend_start, \
		self.client_address, self.client_port = sd

		# First word from the version string.
		self.type = self.version.split()[0]

		##
		# Set standard_conforming_strings
		scstr = self.settings.get('standard_conforming_strings')
		if scstr is None or vi == (8,1):
			# There used to be a warning emitted here.
			# It was noisy, and had little added value
			# over a nice WARNING at the top of the driver documentation.
			pass
		elif scstr.lower() not in ('on','true','yes'):
			self.settings['standard_conforming_strings'] = 'on'

		super().connect()

	def _pq_push(self, xact, controller = None):
		x = self.pq.xact
		if x is not None:
			self.pq.complete()
			if x.fatal is not None:
				self.typio.raise_error(x.error_message)
		if controller is not None:
			self._controller = controller
		self.pq.push(xact)

	# Complete the current protocol transaction.
	def _pq_complete(self):
		pq = self.pq
		x = pq.xact
		if x is not None:
			# There is a running transaction, finish it.
			pq.complete()
			# Raise an error *iff* one occurred.
			if x.fatal is not None:
				self.typio.raise_error(x.error_message, cause = getattr(x, 'exception', None))
			del self._controller

	# Process the next message.
	def _pq_step(self, complete_state = globals()['xact'].Complete):
		pq = self.pq
		x = pq.xact
		if x is not None:
			pq.step()
			# If the protocol transaction was completed by
			# the last step, raise the error *iff* one occurred.
			if x.state is complete_state:
				if x.fatal is not None:
					self.typio.raise_error(x.error_message, cause = getattr(x, 'exception', None))
				del self._controller

	def _receive_async(self,
		msg, controller = None,
		showoption = element.ShowOption.type,
		notice = element.Notice.type,
		notify = element.Notify.type,
	):
		c = controller or getattr(self, '_controller', self)
		typ = msg.type
		if typ == showoption:
			if msg.name == b'client_encoding':
				self.typio.set_encoding(msg.value.decode('ascii'))
			self.settings._notify(msg)
		elif typ == notice:
			m = self.typio.emit_message(msg, creator = c)
		elif typ == notify:
			self._notifies.append(msg)
		else:
			self.typio.emit_client_message(
				element.ClientNotice((
					(b'C', '-1000'),
					(b'S', 'WARNING'),
					(b'M', 'cannot process unrecognized asynchronous message'),
					(b'D', repr(msg)),
				)),
				creator = c
			)

	def clone(self, *args, **kw):
		c = self.__class__(self.connector, *args, **kw)
		c.connect()
		return c

	def notify(self, *channels, **channel_and_payload):
		notifies = ""
		if channels:
			notifies += ';'.join((
				'NOTIFY "' + x.replace('"', '""') + '"' # str() case
				if x.__class__ is not tuple else (
					# tuple() case
					'NOTIFY "' + x[0].replace('"', '""') + """",'""" + \
					x[1].replace("'", "''") + "'"
				)
				for x in channels
			))
			notifies += ';'
		if channel_and_payload:
			notifies += ';'.join((
				'NOTIFY "' + channel.replace('"', '""') + """",'""" + \
				payload.replace("'", "''") + "'"
				for channel, payload in channel_and_payload.items()
			))
			notifies += ';'
		return self.execute(notifies)

	def listening_channels(self):
		if self.version_info[:2] > (8,4):
			return self.sys.listening_channels()
		else:
			return self.sys.listening_relations()

	def listen(self, *channels, len = len):
		qstr = ''
		for x in channels:
			# XXX: hardcoded identifier length?
			if len(x) > 63:
				raise ValueError("channel name too long: " + x)
			qstr += '; LISTEN ' + x.replace('"', '""')
		return self.execute(qstr)

	def unlisten(self, *channels, len = len):
		qstr = ''
		for x in channels:
			# XXX: hardcoded identifier length?
			if len(x) > 63:
				raise ValueError("channel name too long: " + x)
			qstr += '; UNLISTEN ' + x.replace('"', '""')
		return self.execute(qstr)

	def iternotifies(self, timeout = None):
		nm = NotificationManager(self, timeout = timeout)
		for x in nm:
			if x is None:
				yield None
			else:
				for y in x[1]:
					yield y

	def __init__(self, connector, *args, **kw):
		"""
		Create a connection based on the given connector.
		"""
		self.connector = connector
		# raw notify messages
		self._notifies = []
		self.fileno = -1
		self.typio = self.connector.driver.typio(self)
		self.typio.set_encoding('ascii')
		self.settings = Settings(self)
# class Connection

class Connector(pg_api.Connector):
	"""
	All arguments to Connector are keywords. At the very least, user,
	and socket, may be provided. If socket, unix, or process is not
	provided, host and port must be.
	"""
	@property
	def _pq_iri(self):
		return pg_iri.serialize(
			{
				k : v for k,v in self.__dict__.items()
				if v is not None and not k.startswith('_') and k not in (
					'driver', 'category'
				)
			},
			obscure_password = True
		)

	def _e_metas(self):
		yield (None, '[' + self.__class__.__name__ + '] ' + self._pq_iri)

	def __repr__(self):
		keywords = (',' + os.linesep + ' ').join([
			'%s = %r' %(k, getattr(self, k, None)) for k in self.__dict__
			if not k.startswith('_') and getattr(self, k, None) is not None
		])
		return '{mod}.{name}({keywords})'.format(
			mod = type(self).__module__,
			name = type(self).__name__,
			keywords = os.linesep + ' ' + keywords if keywords else ''
		)

	@abstractmethod
	def socket_factory_sequence(self):
		"""
		Generate a list of callables that will be used to attempt to make the
		connection to the server. It is assumed that each factory will produce
		an object with a socket interface that is ready for reading and writing
		data.

		The callables in the sequence must take a timeout parameter.
		"""

	def __init__(self,
		connect_timeout : int = None,
		server_encoding : "server encoding hint for driver" = None,
		sslmode : ('allow', 'prefer', 'require', 'disable') = None,
		sslcrtfile : "filepath" = None,
		sslkeyfile : "filepath" = None,
		sslrootcrtfile : "filepath" = None,
		sslrootcrlfile : "filepath" = None,
		driver = None,
		**kw
	):
		super().__init__(**kw)
		self.driver = driver

		self.server_encoding = server_encoding
		self.connect_timeout = connect_timeout
		self.sslmode = sslmode
		self.sslkeyfile = sslkeyfile
		self.sslcrtfile = sslcrtfile
		self.sslrootcrtfile = sslrootcrtfile
		self.sslrootcrlfile = sslrootcrlfile

		if self.sslrootcrlfile is not None:
			pg_exc.IgnoredClientParameterWarning(
				"certificate revocation lists are *not* checked",
				creator = self,
			).emit()

		# Startup message parameters.
		tnkw = {
			'client_min_messages' : 'WARNING',
		}
		if self.settings:
			s = dict(self.settings)
			if 'search_path' in self.settings:
				sp = s.get('search_path')
				if sp is None:
					self.settings.pop('search_path')
				elif not isinstance(sp, str):
					s['search_path'] = ','.join(
						pg_str.quote_ident(x) for x in sp
					)
			tnkw.update(s)

		tnkw['user'] = self.user
		if self.database is not None:
			tnkw['database'] = self.database

		se = self.server_encoding or 'utf-8'
		##
		# Attempt to accommodate for literal treatment of startup data.
		##
		self._startup_parameters = tuple([
			# All keys go in utf-8. However, ascii would probably be good enough.
			(
				k.encode('utf-8'),
			# If it's a str(), encode in the hinted server_encoding.
			# Otherwise, convert the object(int, float, bool, etc) into a string
			# and treat it as utf-8.
				v.encode(se) if type(v) is str else str(v).encode('utf-8')
			)
			for k, v in tnkw.items()
		])
		self._password = (self.password or '').encode(se)
		self._socket_secure = {
			'keyfile' : self.sslkeyfile,
			'certfile' : self.sslcrtfile,
			'ca_certs' : self.sslrootcrtfile,
		}
# class Connector

class SocketConnector(Connector):
	'abstract connector for using `socket` and `ssl`'
	@abstractmethod
	def socket_factory_sequence(self):
		"""
		Return a sequence of `SocketFactory`s for a connection to use to connect
		to the target host.
		"""

	def create_socket_factory(self, **params):
		return SocketFactory(**params)

class IPConnector(SocketConnector):
	def socket_factory_sequence(self):
		return self._socketcreators

	def socket_factory_params(self, host, port, ipv, **kw):
		if ipv != self.ipv:
			raise TypeError("'ipv' keyword must be '%d'" % self.ipv)
		if host is None:
			raise TypeError("'host' is a required keyword and cannot be 'None'")
		if port is None:
			raise TypeError("'port' is a required keyword and cannot be 'None'")

		return {'socket_create': (self.address_family, socket.SOCK_STREAM),
				'socket_connect': (host, int(port))}

	def __init__(self, host, port, ipv, **kw):
		params = self.socket_factory_params(host, port, ipv, **kw)
		self.host, self.port = params['socket_connect']
		# constant socket connector
		self._socketcreator = self.create_socket_factory(**params)
		self._socketcreators = (self._socketcreator,)
		super().__init__(**kw)

class IP4(IPConnector):
	'Connector for establishing IPv4 connections'
	ipv = 4
	address_family = socket.AF_INET

	def __init__(self,
		host : "IPv4 Address (str)" = None,
		port : int = None,
		ipv = 4,
		**kw
	):
		super().__init__(host, port, ipv, **kw)

class IP6(IPConnector):
	'Connector for establishing IPv6 connections'
	ipv = 6
	address_family = socket.AF_INET6

	def __init__(self,
		host : "IPv6 Address (str)" = None,
		port : int = None,
		ipv = 6,
		**kw
	):
		super().__init__(host, port, ipv, **kw)

class Unix(SocketConnector):
	'Connector for establishing unix domain socket connections'
	def socket_factory_sequence(self):
		return self._socketcreators

	def socket_factory_params(self, unix):
		if unix is None:
			raise TypeError("'unix' is a required keyword and cannot be 'None'")

		return {'socket_create': (socket.AF_UNIX, socket.SOCK_STREAM),
				'socket_connect': unix}

	def __init__(self, unix = None, **kw):
		params = self.socket_factory_params(unix)
		self.unix = params['socket_connect']
		# constant socket connector
		self._socketcreator = self.create_socket_factory(**params)
		self._socketcreators = (self._socketcreator,)
		super().__init__(**kw)

class Host(SocketConnector):
	"""
	Connector for establishing hostname based connections.

	This connector exercises socket.getaddrinfo.
	"""
	def socket_factory_sequence(self):
		"""
		Return a list of `SocketCreator`s based on the results of
		`socket.getaddrinfo`.
		"""
		return [
			# (AF, socktype, proto), (IP, Port)
			self.create_socket_factory(**(self.socket_factory_params(x[0:3], x[4][:2],
																	self._socket_secure)))
			for x in socket.getaddrinfo(
				self.host, self.port, self._address_family, socket.SOCK_STREAM
			)
		]

	def socket_factory_params(self, socktype, address, sslparams):
		return {'socket_create': socktype,
				'socket_connect': address,
				'socket_secure': sslparams}

	def __init__(self,
		host : str = None,
		port : (str, int) = None,
		ipv : int = None,
		address_family : "address family to use(AF_INET,AF_INET6)" = None,
		**kw
	):
		if host is None:
			raise TypeError("'host' is a required keyword")
		if port is None:
			raise TypeError("'port' is a required keyword")

		if address_family is not None and ipv is not None:
			raise TypeError("'ipv' and 'address_family' on mutually exclusive")

		if ipv is None:
			self._address_family = address_family or socket.AF_UNSPEC
		elif ipv == 4:
			self._address_family = socket.AF_INET
		elif ipv == 6:
			self._address_family = socket.AF_INET6
		else:
			raise TypeError("unknown IP version selected: 'ipv' = " + repr(ipv))
		self.host = host
		self.port = port
		super().__init__(**kw)

class Driver(pg_api.Driver):
	def _e_metas(self):
		yield (None, type(self).__module__ + '.' + type(self).__name__)

	def ip4(self, **kw):
		return IP4(driver = self, **kw)

	def ip6(self, **kw):
		return IP6(driver = self, **kw)

	def host(self, **kw):
		return Host(driver = self, **kw)

	def unix(self, **kw):
		return Unix(driver = self, **kw)

	def fit(self,
		unix = None,
		host = None,
		port = None,
		**kw
	) -> Connector:
		"""
		Create the appropriate `postgresql.api.Connector` based on the
		parameters.

		This also protects against mutually exclusive parameters.
		"""
		if unix is not None:
			if host is not None:
				raise TypeError("'unix' and 'host' keywords are exclusive")
			if port is not None:
				raise TypeError("'unix' and 'port' keywords are exclusive")
			return self.unix(unix = unix, **kw)
		else:
			if host is None or port is None:
				raise TypeError("'host' and 'port', or 'unix' must be supplied")
			# We have a host and a port.
			# If it's an IP address, IP4 or IP6 should be selected.
			if ':' in host:
				# There's a ':' in host, good chance that it's IPv6.
				try:
					socket.inet_pton(socket.AF_INET6, host)
					return self.ip6(host = host, port = port, **kw)
				except (socket.error, NameError):
					pass

			# Not IPv6, maybe IPv4...
			try:
				socket.inet_aton(host)
				# It's IP4
				return self.ip4(host = host, port = port, **kw)
			except socket.error:
				pass

			# neither host, nor port are None, probably a hostname.
			return self.host(host = host, port = port, **kw)

	def connect(self, **kw) -> Connection:
		"""
		For information on acceptable keywords, see:

			`postgresql.documentation.driver`:Connection Keywords
		"""
		c = self.fit(**kw)()
		c.connect()
		return c

	def __init__(self, connection = Connection, typio = TypeIO):
		self.connection = connection
		self.typio = typio
