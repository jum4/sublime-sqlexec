##
# .protocol.client3
##
"""
Protocol version 3.0 client and tools.
"""
import os
import weakref
from .buffer import pq_message_stream
from . import element3 as element
from . import xact3 as xact

__all__ = ('Connection',)

client_detected_protocol_error = element.ClientError((
	(b'S', 'FATAL'),
	(b'C', '08P01'),
	(b'M', "wire-data caused exception in protocol transaction"),
	(b'H', "Protocol error detected."),
))

client_connect_timeout = element.ClientError((
	(b'S', 'FATAL'),
	(b'C', '--TOE'),
	(b'M', "connect timed out"),
))

not_pq_error = element.ClientError((
	# ProtocolError
	(b'S', 'FATAL'),
	(b'C', '08P01'),
	(b'M', 'server did not support SSL negotiation'),
	(b'H', 'The server is probably not PostgreSQL.'),
))

no_ssl_error = element.ClientError((
	(b'S', 'FATAL'),
	# InsecurityError
	(b'C', '--SEC'),
	(b'M', 'SSL was required, and the server could not accommodate'),
))

# Details in __context__
ssl_failed_error = element.ClientError((
	(b'S', 'FATAL'),
	# InsecurityError
	(b'C', '--SEC'),
	(b'M', 'SSL negotiation caused exception'),
))

# failed to complete the connection, but no error set.
# indicates a programmer error.
partial_connection_error = element.ClientError((
	(b'S', 'FATAL'),
	(b'C', '--XXX'),
	(b'M', "failed to complete negotiation"),
	(b'H',	"Negotiation failed to completed, but no " \
			"error was attributed on the connection."),
))

eof_error = element.ClientError((
	(b'S', 'FATAL'),
	(b'C', '08006'),
	(b'M', 'unexpected EOF from server'),
	(b'D',	"Zero-length read from the connection's socket."),
))

class Connection(object):
	"""
	A PQv3 connection.

	Operations are designed to not raise exceptions. The user of the
	connection must check for failures. This is done to encourage users
	to use their own Exception hierarchy.
	"""
	_tracer = None
	def tracer():
		def fget(self):
			return self._tracer
		def fset(self, value):
			self._tracer = value
			self.write_messages = self.traced_write_messages
			self.read_messages = self.traced_read_messages
		def fdel(self):
			del self._tracer
			self.write_messages = self.standard_write_messages
			self.read_messages = self.standard_read_messages
		doc = 'Callable object to pass protocol trace strings to. '\
			'(Normally a write method.)'
		return locals()
	tracer = property(**tracer())

	def synchronize(self):
		"""
		Explicitly send a Synchronize message to the backend.
		Useful for forcing the completion of lazily processed transactions.

		NOTE: This will not cause trash to be taken out.
		"""
		if self.xact is not None:
			self.complete()
		x = xact.Instruction((element.SynchronizeMessage,))
		self.xact = x
		self.complete()

	def interrupt(self, timeout = None):
		cq = element.CancelRequest(self.backend_id, self.key).bytes()
		s = self.socket_factory(timeout = timeout)
		try:
			s.sendall(cq)
		finally:
			s.close()

	def connect(self, ssl = None, timeout = None):
		"""
		Establish the connection to the server.

		If `ssl` is None, the socket will not be secured.
		If `ssl` is True, the socket will be secured, but it will
		close the connection and return if SSL is not available.
		If `ssl` is False, the socket will attempt to be secured, but
		will continue even in the event of a server that does not
		support SSL.

		`timeout` will be passed directly to the configured `socket_factory`.
		"""
		if hasattr(self, 'socket'):
			# If there's a socket attribute it normally means
			# that the connection has already been connected.
			# Successfully or not; doesn't matter.
			return

		# The existence of the socket attribute indicates an attempt was made.
		self.socket = None
		try:
			self.socket = self.socket_factory(timeout = timeout)
		except (
			self.socket_factory.timeout_exception,
			self.socket_factory.fatal_exception
		) as err:
			self.xact.state = xact.Complete
			self.xact.fatal = True
			self.xact.exception = err
			if self.socket_factory.timed_out(err):
				self.xact.error_message = client_connect_timeout
			else:
				errmsg = self.socket_factory.fatal_exception_message(err)
				# It's an error that occurred during socket creation/connection.
				# Even if there isn't a known fatal message,
				# identify it as fatal and set an ambiguous message.
				self.xact.error_message = element.ClientError((
					(b'S', 'FATAL'),
					# ConnectionRejectionError
					(b'C', '08004'),
					(b'M', errmsg or "could not connect"),
				))
			return

		if ssl is not None:
			# if ssl is True, ssl is *required*
			# if ssl is False, ssl will be tried, but not required
			# if ssl is None, no SSL negotiation will happen
			self.ssl_negotiation = supported = self.negotiate_ssl()

			# b'S' or b'N' was *not* received.
			if supported is None:
				# probably not PQv3..
				self.xact.fatal = True
				self.xact.error_message = not_pq_error
				self.xact.state = xact.Complete
				return

			# b'N' was received, but ssl is required.
			if not supported and ssl is True:
				# ssl is required..
				self.xact.fatal = True
				self.xact.error_message = no_ssl_error
				self.xact.state = xact.Complete
				return

			if supported:
				# Make an SSL connection.
				try:
					self.socket = self.socket_factory.secure(self.socket)
				except Exception as err:
					# Any exception marks a failure.
					self.xact.exception = err
					self.xact.fatal = True
					self.xact.state = xact.Complete
					self.xact.error_message = ssl_failed_error
					return
		# time to negotiate
		negxact = self.xact
		self.complete()
		if negxact.state is xact.Complete and negxact.fatal is None:
			self.key = negxact.killinfo.key
			self.backend_id = negxact.killinfo.pid
		elif not hasattr(self.xact, 'error_message'):
			# if it's not complete, something strange happened.
			# make sure to clean up...
			self.xact.fatal = True
			self.xact.state = xact.Complete
			self.xact.error_message = partial_connection_error

	def negotiate_ssl(self) -> (bool, None):
		"""
		Negotiate SSL

		If SSL is available--received b'S'--return True.
		If SSL is unavailable--received b'N'--return False.
		Otherwise, return None. Indicates non-PQv3 endpoint.
		"""
		r = element.NegotiateSSLMessage.bytes()
		while r:
			r = r[self.socket.send(r):]
		status = self.socket.recv(1)
		if status == b'S':
			return True
		elif status == b'N':
			return False
		# probably not postgresql.
		return None

	def read_into(self, Complete = xact.Complete):
		"""
		read data from the wire and write it into the message buffer.
		"""
		BUFFER_HAS_MSG = self.message_buffer.has_message
		BUFFER_WRITE_MSG = self.message_buffer.write
		RECV_DATA = self.socket.recv
		RECV_BYTES = self.recvsize
		XACT = self.xact
		while not BUFFER_HAS_MSG():
			if self.read_data is not None:
				BUFFER_WRITE_MSG(self.read_data)
				self.read_data = None
				# If the read_data satisfied a message,
				# no more data should be read.
				continue
			try:
				self.read_data = RECV_DATA(RECV_BYTES)
			except self.socket_factory.fatal_exception as e:
				msg = self.socket_factory.fatal_exception_message(e)
				if msg is not None:
					XACT.state = Complete
					XACT.fatal = True
					XACT.exception = e
					XACT.error_message = element.ClientError((
						(b'S', 'FATAL'),
						(b'C', '08006'),
						(b'M', msg),
					))
					return False
				else:
					# It's probably a non-fatal error,
					# timeout or try again..
					raise

			##
			# nothing read from a blocking socket? it's over.
			if self.read_data == b'':
				XACT.state = Complete
				XACT.fatal = True
				XACT.error_message = eof_error
				return False

			# Got data. Put it in the buffer and clear read_data.
			self.read_data = BUFFER_WRITE_MSG(self.read_data)
		return True

	def standard_read_messages(self):
		'read more messages into self.read when self.read is empty'
		r = True
		if not self.read:
			# get more data from the wire and
			# write it into the message buffer.
			r = self.read_into()
			self.read = self.message_buffer.read()
		return r
	read_messages = standard_read_messages

	def send_message_data(self):
		"""
		send all `message_data`.

		If an exception occurs, it will check if the exception
		is fatal or not.
		"""
		SEND_DATA = self.socket.send
		try:
			while self.message_data:
				# Send data while there is data to send.
				self.message_data = self.message_data[
					SEND_DATA(self.message_data):
				]
		except self.socket_factory.fatal_exception as e:
			msg = self.socket_factory.fatal_exception_message(e)
			if msg is not None:
				# it's fatal.
				self.xact.state = xact.Complete
				self.xact.fatal = True
				self.xact.exception = e
				self.xact.error_message = element.ClientError((
					(b'S', 'FATAL'),
					(b'C', '08006'),
					(b'M', msg),
				))
				return False
			else:
				# It wasn't fatal, so just raise
				raise
		return True

	def standard_write_messages(self, messages,
		cat_messages = element.cat_messages
	):
		'protocol message writer'
		if self.writing is not self.written:
			self.message_data += cat_messages(self.writing)
			self.written = self.writing

		if messages is not self.writing:
			self.writing = messages
			self.message_data += cat_messages(self.writing)
			self.written = self.writing
		return self.send_message_data()
	write_messages = standard_write_messages

	def traced_write_messages(self, messages):
		'message_writer used when tracing'
		for msg in messages:
			t = getattr(msg, 'type', None)
			if t is not None:
				data_out = msg.bytes()
				self._tracer('↑ {type}({lend}): {data}{nl}'.format(
					type = repr(t)[2:-1],
					lend = len(data_out),
					data = repr(data_out),
					nl = os.linesep
				))
			else:
				# It's not a message instance, so assume raw data.
				self._tracer('↑__(%d): %r%s' %(
					len(msg), msg, os.linesep
				))
		return self.standard_write_messages(messages)

	def traced_read_messages(self):
		'message_reader used when tracing'
		r = self.standard_read_messages()
		for msg in self.read:
			self._tracer('↓ %r(%d): %r%s' %(
				msg[0], len(msg[1]), msg[1], os.linesep)
			)
		return r

	def take_out_trash(self):
		"""
		close cursors and statements slated for closure.
		"""
		xm = []
		cursors = 0
		for x in self.garbage_cursors:
			xm.append(element.ClosePortal(x))
			cursors += 1
		statements = 0
		for x in self.garbage_statements:
			xm.append(element.CloseStatement(x))
			statements += 1
		xm.append(element.SynchronizeMessage)
		x = xact.Instruction(xm)
		self.xact = x
		del self.garbage_cursors[:cursors]
		del self.garbage_statements[:statements]
		self.complete()

	def push(self, x):
		"""
		setup the given transaction to be processed.
		"""
		# Push any queued closures onto the transaction or a new transaction.
		if x.state is xact.Complete:
			# It's already complete.
			return
		if self.xact is not None:
			self.complete()
		if self.xact is None:
			if self.garbage_statements or self.garbage_cursors:
				# This *has* to be done before a new transaction
				# is pushed.
				self.take_out_trash()
			if self.xact is None:
				# set it as the current transaction and begin
				self.xact = x
				# start it up
				self.step()

	def step(self):
		"""
		Make a single transition on the transaction.

		This should be used during COPY TO STDOUT or large result sets
		to stream information out.
		"""
		x = self.xact
		try:
			dir, op = x.state
			if dir is xact.Sending:
				self.write_messages(x.messages)
				# The "op" callable will either switch the state, or
				# set the 'messages' attribute with a new sequence
				# of message objects for more writing.
				op()
			elif dir is xact.Receiving:
				self.read_messages()
				self.read = self.read[op(self.read):]
				self.state = getattr(x, 'last_ready', self.state)
			else:
				raise RuntimeError(
					"unexpected PQ transaction state: " + repr(dir)
				)
		except self.socket_factory.try_again_exception as e:
			# Unlike _complete, this catches at the outermost level
			# as there is no loop here for more transitioning.
			if self.socket_factory.try_again(e):
				# Can't read or write, ATM? Consider it a transition. :(
				return
			else:
				raise
		if x.state is xact.Complete and \
		getattr(self.xact, 'fatal', None) is not True:
			# only remove the transaction if it's *not* fatal
			self.xact = None

	def complete(self):
		'complete the current transaction'
		# Continue to transition until all transactions have been
		# completed, or an exception occurs that does not signal retry.
		x = self.xact
		R = xact.Receiving
		S = xact.Sending
		C = xact.Complete
		READ_MORE = self.read_messages
		WRITE_MESSAGES = self.write_messages
		while x.state is not C:
			try:
				while x.state[0] is R:
					if READ_MORE():
						self.read = self.read[x.state[1](self.read):]
				# push() always takes one step, so it is likely that
				# the transaction is done sending out data by the time
				# complete() is called.
				while x.state[0] is S:
					if WRITE_MESSAGES(x.messages):
						x.state[1]()
					# Multiple calls to get() without signaling
					# completion *should* yield the same set over
					# and over again.
			except self.socket_factory.try_again_exception as e:
				if not self.socket_factory.try_again(e):
					raise
			except Exception as proto_exc:
				# If an exception is raised here, it's a protocol or a programming error.
				# XXX: It may be useful to have this closer to the actual
				# message so that a more informative message can be given.
				x.fatal = True
				x.state = xact.Complete
				x.exception = proto_exc
				x.error_message = client_detected_protocol_error
				self.state = b''
				return
		self.state = getattr(x, 'last_ready', self.state)
		if getattr(x, 'fatal', None) is not True:
			# only remove the transaction if it's *not* fatal
			self.xact = None

	def register_cursor(self, cursor, pq_cursor_id):
		trash = self.trash_cursor
		self.cursors[pq_cursor_id] = weakref.ref(cursor, lambda ref: trash(pq_cursor_id))

	def trash_cursor(self, pq_cursor_id):
		try:
			del self.cursors[pq_cursor_id]
		except KeyError:
			pass
		self.garbage_cursors.append(pq_cursor_id)

	def register_statement(self, statement, pq_statement_id):
		trash = self.trash_statement
		self.statements[pq_statement_id] = weakref.ref(statement, lambda ref: trash(pq_statement_id))

	def trash_statement(self, pq_statement_id):
		try:
			del self.statements[pq_statement_id]
		except KeyError:
			pass
		self.garbage_statements.append(pq_statement_id)

	def __str__(self):
		if hasattr(self, 'ssl_negotiation'):
			if self.ssl_negotiation is True:
				ssl = 'SSL'
			elif self.ssl_negotiation is False:
				ssl = 'NOSSL after SSL'
		else:
			ssl = 'NOSSL'

		excstr = ''.join(self.exception_string(type(self.exception), self.exception))
		return str(self.socket_factory) \
			+ ' -> (' + ssl + ')' \
			+ os.linesep + excstr.strip()

	def __init__(self, socket_factory, startup, password = b'',):
		"""
		Create a connection.

		This does not establish the connection, it only initializes it.
		"""
		self.key = None
		self.backend_id = None

		self.socket_factory = socket_factory
		self.xact = xact.Negotiation(
			element.Startup(startup), password
		)

		self.cursors = {}
		self.statements = {}

		self.garbage_statements = []
		self.garbage_cursors = []

		self.message_buffer = pq_message_stream()
		self.recvsize = 8192

		self.read = ()
		# bytes received.
		self.read_data = None

		# serialized message data to be written
		self.message_data = b''
		# messages to be written.
		self.writing = None
		# messages that have already been transformed into bytes.
		# (used to detect whether messages have already been written)
		self.written = None

		self.state = 'INITIALIZED'
