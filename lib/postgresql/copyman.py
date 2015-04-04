##
# .copyman - COPY manager
##
"""
Manage complex COPY operations; one-to-many COPY streaming.

Primarily this module houses the `CopyManager` class, and the `transfer`
function for a high-level interface to using the `CopyManager`.
"""
import sys
from abc import abstractmethod, abstractproperty
from collections import Iterator
from .python.element import Element, ElementSet
from .python.structlib import ulong_unpack, ulong_pack
from .protocol.buffer import pq_message_stream
from .protocol.element3 import CopyData, CopyDone, Complete, cat_messages
from .protocol.xact3 import Complete as xactComplete

#: 10KB buffer for COPY messages by default.
default_buffer_size = 1024 * 10

class Fault(Exception):
	pass

class ProducerFault(Fault):
	"""
	Exception raised when the Producer caused an exception.

	Normally, Producer faults are fatal.
	"""
	def __init__(self, manager):
		self.manager = manager

	def __str__(self):
		return "producer raised exception"

class ReceiverFault(Fault):
	"""
	Exception raised when Receivers cause an exception.

	Faults should be trapped if recovery from an exception is
	possible, or if the failed receiver is optional to the succes of the
	operation.

	The 'manager' attribute is the CopyManager that raised the fault.

	The 'faults' attribute is a dictionary mapping the receiver to the exception
	instance raised.
	"""
	def __init__(self, manager, faults):
		self.manager = manager
		self.faults = faults

	def __str__(self):
		return "{0} faults occurred".format(len(self.faults))

class CopyFail(Exception):
	"""
	Exception thrown by the CopyManager when the COPY operation failed.

	The 'manager' attribute the CopyManager that raised the CopyFail.

	The 'reason' attribute is a string indicating why it failed.

	The 'receiver_faults' attribute is a mapping of receivers to exceptions that were
	raised on exit.

	The 'producer_fault' attribute specifies if the producer raise an exception
	on exit.
	"""
	def __init__(self, manager, reason = None,
		receiver_faults = None,
		producer_fault = None,
	):
		self.manager = manager
		self.reason = reason
		self.receiver_faults = receiver_faults or {}
		self.producer_fault = producer_fault

	def __str__(self):
		return self.reason or 'copy aborted'

# The identifier for PQv3 copy data.
PROTOCOL_PQv3 = "PQv3"
# The identifier for iterables of copy data sequences.
# iter([[row1, row2], [row3, row4]])
PROTOCOL_CHUNKS = "CHUNKS"
# The protocol identifier for NULL producers and receivers.
PROTOCOL_NULL = None

class ChunkProtocol(object):
	__slots__ = ('buffer',)
	def __init__(self):
		self.buffer = pq_message_stream()

	def __call__(self, data):
		self.buffer.write(bytes(data))
		return [x[1] for x in self.buffer.read()]

# Null protocol mapping.
def EmptyView(arg):
	return memoryview(b'')
def EmptyList(arg):
	return []
def ReturnNone(arg):
	return None
# Zero-Transformation
def NoTransformation(arg):
	return arg

# Copy protocols being at the Python level; *not* wire/serialization format.
copy_protocol_mappings = {
	# PQv3 -> Chunks
	(PROTOCOL_PQv3, PROTOCOL_CHUNKS) : ChunkProtocol,
	# Chunks -> PQv3
	(PROTOCOL_CHUNKS, PROTOCOL_PQv3) : lambda: cat_messages,
	# Null Producers and Receivers
	(PROTOCOL_NULL, PROTOCOL_PQv3) : lambda: EmptyView,
	(PROTOCOL_NULL, PROTOCOL_CHUNKS) : lambda: EmptyList,
	(PROTOCOL_PQv3, PROTOCOL_NULL) : lambda: ReturnNone,
	(PROTOCOL_CHUNKS, PROTOCOL_NULL) : lambda: ReturnNone,
	# Zero Transformations
	(PROTOCOL_NULL, PROTOCOL_NULL) : lambda: NoTransformation,
	(PROTOCOL_CHUNKS, PROTOCOL_CHUNKS) : lambda: NoTransformation,
	(PROTOCOL_PQv3, PROTOCOL_PQv3) : lambda: NoTransformation,
}

# Used to manage the conversions of COPY data.
# Notably, chunks -> PQv3 or PQv3 -> chunks.
class CopyTransformer(object):
	__slots__ = ('current', 'transformers', 'get')
	def __init__(self, source_protocol, target_protocols):
		self.current = {}
		self.transformers = {
			x : copy_protocol_mappings[(source_protocol, x)]()
			for x in set(target_protocols)
		}
		self.get = self.current.__getitem__

	def __call__(self, data):
		for protocol, transformer in self.transformers.items():
			self.current[protocol] = transformer(data)

##
# This is the object that does the magic.
# It tracks the state of the wire.
# It ends when non-COPY data is found.
class WireState(object):
	"""
	Manages the state of the wire.

	This class manages three possible positions:

	 1. Between wire messages
	 2. Inside message header
	 3. Inside message (with complete header)

	The wire state will become unusable when the configured condition is True.
	"""
	__slots__ = ('remaining_bytes', 'size_fragment', 'final_view', 'condition',)

	def update(self, view, getlen = ulong_unpack, len = len):
		"""
		Given the state of the COPY and new data, advance the position on the
		COPY stream.
		"""
		# Only usable until the terminating condition.
		if self.final_view is not None:
			raise RuntimeError("wire state encountered exceptional condition")

		nmessages = 0

		# State carried over from prior run.
		remaining_bytes = self.remaining_bytes
		size_fragment = self.size_fragment

		# Terminating condition.
		CONDITION = self.condition

		# Is it a continuation of a message header?
		if remaining_bytes == -1:
			##
			# Inside message header; after message type.
			# Continue adding to the 'size_fragment'
			# until there are four bytes to unpack.
			##
			o = len(size_fragment)
			size_fragment += bytes(view[:4-o])
			if len(size_fragment) == 4:
				# The size fragment is completed; only part
				# of the fragment remains to be consumed.
				remaining_bytes = getlen(size_fragment) - o
				size_fragment = b''
			else:
				assert len(size_fragment) < 4
				# size_fragment got updated..

		if remaining_bytes >= 0:
			vlen = len(view)
			while True:
				if remaining_bytes:
					##
					# Inside message body. Message length has been unpacked.
					##
					view = view[remaining_bytes:]
					# How much is remaining now?
					rb = remaining_bytes - vlen
					if rb <= 0:
						# Finished it.
						vlen = -rb
						remaining_bytes = 0
						nmessages += 1
					else:
						vlen = 0
						remaining_bytes = rb
				##
				# In between protocol messages.
				##
				if not view:
					# no more data to analyze
					break
				# There is at least one byte in the view.
				if CONDITION(view[0]):
					# State is dead now.
					# User needs to handle unexpected message, then continue.
					self.final_view = view
					assert remaining_bytes == 0
					break
				if vlen < 5:
					# Header continuation.
					remaining_bytes = -1
					view = view[1:]
					size_fragment += bytes(view)
					# Not enough left for the header of the next message?
					break
				# Update remaining_bytes to include the header, and start over.
				remaining_bytes = getlen(view[1:5]) + 1

		# Update the state for the next update.
		self.remaining_bytes, self.size_fragment = (
			remaining_bytes, size_fragment,
		)
		# Emit the number of messages "consumed" this round.
		return nmessages

	def __init__(self, condition = (CopyData.type[0].__ne__ if isinstance(memoryview(b'f')[0], int) else CopyData.type.__ne__)):
		self.remaining_bytes = 0
		self.size_fragment = b''
		self.final_view = None
		self.condition = condition

class Fitting(Element):
	_e_label = 'FITTING'

	def _e_metas(self):
		yield None, '[' + self.state + ']'

	@abstractproperty
	def protocol(self):
		"""
		The COPY data format produced or consumed.
		"""

	# Used to setup the Receiver/Producer
	def __enter__(self):
		pass

	# Used to tear down the Receiver/Producer
	def __exit__(self, typ, val, tb):
		pass

class Producer(Fitting, Iterator):
	_e_label = 'PRODUCER'

	def _e_metas(self):
		for x in super()._e_metas():
			yield x
		yield 'data', str(self.total_bytes / (1024**2)) + 'MB'
		yield 'messages', self.total_messages
		yield 'average size', (self.total_bytes / self.total_messages)

	def __init__(self):
		self.total_messages = 0
		self.total_bytes = 0

	@abstractmethod
	def realign(self):
		"""
		Method implemented by producers that emit COPY data that is not
		guaranteed to be aligned.

		This is only necessary in failure cases where receivers still need more
		data to complete the message.
		"""

	@abstractmethod
	def __next__(self):
		"""
		Produce the next set of data.
		"""

class Receiver(Fitting):
	_e_label = 'RECEIVER'

	@abstractmethod
	def transmit(self):
		"""
		Finish the reception of the accepted data.
		"""

	@abstractmethod
	def accept(self, data):
		"""
		Take the data object to be processed.
		"""

class NullProducer(Producer):
	"""
	Produces no copy data.
	"""
	_e_factors = ()
	protocol = PROTOCOL_NULL

	def realign(self):
		# Never needs to realigned.
		pass

	def __next__(self):
		raise StopIteration

class IteratorProducer(Producer):
	_e_factors = ('iterator',)
	protocol = PROTOCOL_CHUNKS

	def __init__(self, iterator):
		self.iterator = iter(iterator)
		self.__next__ = self.iterator.__next__
		super().__init__()

	def realign(self):
		# Never needs to realign; data is emitted on message boundaries.
		pass

	def __next__(self, next = next):
		n = next(self.iterator)
		self.total_messages += len(n)
		self.total_bytes += sum(map(len, n))
		return n

class ProtocolProducer(Producer):
	"""
	Producer using a PQv3 data stream.

	Normally, this class needs to be subclassed as it assumes that the given
	recv_into function will write COPY messages.
	"""
	protocol = PROTOCOL_PQv3

	@abstractmethod
	def recover(self, view):
		"""
		Given a view containing data read from the wire, recover the
		controller's state.

		This needs to be implemented by subclasses in order for the
		ProtocolReceiver to pass control back to the original state machine.
		"""

	##
	# When a COPY is interrupted, this can be used to accommodate
	# the original state machine to identify the message boundaries.
	def realign(self):
		s = self._state

		if s is None:
			# It's already aligned.
			self.nextchunk = iter(()).__next__
			return

		if s.final_view:
			# It was at the end or non-COPY.
			for_producer = bytes(s.final_view)
			for_receivers = b''
		elif s.remaining_bytes == -1:
			# In the middle of a message header.
			for_producer = CopyData.type + s.size_fragment
			# receivers:
			header = (self._state.size_fragment.ljust(3, b'\x00') + b'\x04')
			# Don't include the already sent parts.
			buf = header[len(self._state.size_fragment):]
			bodylen = ulong_unpack(header) - 4
			# This will often cause an invalid copy data error,
			# but it doesn't matter much because we will issue a copy fail.
			buf += b'\x00' * bodylen
			for_receivers = buf
		elif s.remaining_bytes > 0:
			# In the middle of a message.
			for_producer = CopyData.type + ulong_pack(s.remaining_bytes + 4)
			for_receivers = b'\x00' * self._state.remaining_bytes
		else:
			for_producer = for_receivers = b''

		self.recover(for_producer)
		if for_receivers:
			self.nextchunk = iter((for_receivers,)).__next__
		else:
			self.nextchunk = iter(()).__next__

	def process_copy_data(self, view):
		self.total_messages += self._state.update(view)
		if self._state.final_view is not None:
			# It's not COPY data.
			fv = self._state.final_view
			# Only publish up to the final_view.
			if fv:
				view = view[:-len(fv)]
			# The next next() will handle the async, error, or completion.
			self.recover(fv)
			self._state = None
		self.total_bytes += len(view)
		return view

	# Given a view, begin tracking the state of the wire.
	def track_state(self, view):
		self._state = WireState()
		self.nextchunk = self.recv_view
		return self.process_copy_data(view)

	# The usual method for receiving more data.
	def recv_view(self):
		view = self.buffer_view[:self.recv_into(self.buffer, self.buffer_size)]
		if not view:
			# Zero read; let the subclass handle the situation.
			self.recover(memoryview(b''))
			return self.nextchunk()
		view = self.process_copy_data(view)
		return view

	def nextchunk(self):
		raise RuntimeError("producer not properly initialized")

	def __next__(self):
		return self.nextchunk()

	def __init__(self,
		recv_into : "callable taking writable buffer and size",
		buffer_size = default_buffer_size
	):
		super().__init__()
		self.recv_into = recv_into
		self.buffer_size = buffer_size
		self.buffer = bytearray(buffer_size)
		self.buffer_view = memoryview(self.buffer)
		self._state = None

class StatementProducer(ProtocolProducer):
	_e_factors = ('statement', 'parameters',)

	def _e_metas(self):
		for x in super()._e_metas():
			yield x

	@property
	def state(self):
		if self._chunks is None:
			return 'created'
		return 'producing'

	def count(self):
		return self._chunks.count()

	def command(self):
		return self._chunks.command()

	def __init__(self, statement, *args, **kw):
		super().__init__(statement.database.pq.socket.recv_into, **kw)
		self.statement = statement
		self.parameters = args
		self._chunks = None

	##
	# Take any data held by the statement's chunks and connection.
	def confiscate(self, next = next):
		current = []
		try:
			while not current:
				current.extend(next(self._chunks))
		except StopIteration:
			if not current:
				# End of COPY.
				raise
		pq = self._chunks.database.pq
		buffer = cat_messages(current) + pq.message_buffer.getvalue() + (pq.read_data or b'')
		view = memoryview(buffer)
		pq.read_data = None
		pq.message_buffer.truncate()
		# Reconstruct the buffer from the already parsed lines.
		r = self.track_state(view)
		# XXX: Better way? Probably shouldn't do the full track_state if complete..
		if self._chunks._xact.state is xactComplete:
			# It's over, don't hand off to recv_view.
			self.nextchunk = self.confiscate
			assert self._state.final_view is None
		return r

	def recover(self, view):
		# Method used when non-COPY data is found.
		self._chunks.database.pq.message_buffer.write(bytes(view))
		self.nextchunk = self.confiscate

	def __enter__(self):
		super().__enter__()
		if self._chunks is not None:
			raise RuntimeError("receiver already used")
		self._chunks = self.statement.chunks(*self.parameters)
		# Start by confiscating the connection state.
		self.nextchunk = self.confiscate

	def __exit__(self, typ, val, tb):
		if typ is None or issubclass(typ, Exception):
			db = self.statement.database
			if not db.closed and self._chunks._xact is not None:
				# The COPY transaction is still happening,
				# force an interrupt if the connection still exists.
				db.interrupt()
				if db.pq.xact:
					# Raise, CopyManager should trap.
					db._pq_complete()
		super().__exit__(typ, val, tb)

class NullReceiver(Receiver):
	_e_factors = ()
	protocol = PROTOCOL_NULL
	state = 'null'

	def transmit(self):
		# Nothing to do.
		pass

	def accept(self, data):
		pass

class ProtocolReceiver(Receiver):
	protocol = PROTOCOL_PQv3
	__slots__ = ('send', 'view')

	def __init__(self, send):
		super().__init__()
		self.send = send
		self.view = memoryview(b'')

	def accept(self, data):
		self.view = data

	def transmit(self):
		while self.view:
			self.view = self.view[self.send(self.view):]

	def __enter__(self):
		return self

	def __exit__(self, typ, val, tb):
		pass

class StatementReceiver(ProtocolReceiver):
	_e_factors = ('statement', 'parameters',)
	__slots__ = ProtocolReceiver.__slots__ + _e_factors + ('xact',)

	def _e_metas(self):
		yield None, '[' + self.state + ']'

	def __init__(self, statement, *parameters):
		self.statement = statement
		self.parameters = parameters
		self.xact = None
		super().__init__(statement.database.pq.socket.send,)

	# XXX: A bit of a hack...
	# This is actually a good indication that statements need a .copy()
	# execution method for producing a "CopyCursor" that reads or writes.
	class WireReady(BaseException):
		pass
	def raise_wire_ready(self):
		raise self.WireReady()
		yield None

	def __enter__(self, iter = iter):
		super().__enter__()
		# Get the connection in the COPY state.
		try:
			self.statement.load_chunks(
				iter(self.raise_wire_ready()), *self.parameters
			)
		except self.WireReady:
			# It's a BaseException; nothing should trap it.
			# Note the transaction object; we'll use it on exit.
			self.xact = self.statement.database.pq.xact

	def __exit__(self, typ, val, tb):
		if self.xact is None:
			# Nothing to do.
			return super().__exit__(typ, val, tb)

		if self.view:
			# The realigned producer emitted the necessary
			# data for message boundary alignment.
			#
			# In this case, we unconditionally fail.
			pq = self.statement.database.pq
			# There shouldn't be any message_data, atm.
			pq.message_data = bytes(self.view)
			self.statement.database._pq_complete()
			# It is possible for a non-alignment view to exist in cases of
			# faults. However, exit should *not* be called in those cases.
			##
		elif typ is None:
			# Success?
			self.xact.messages = self.xact.CopyDoneSequence
			# If not, this will blow up.
			self.statement.database._pq_complete()
			# Find the complete message for command and count.
			for x in self.xact.messages_received():
				if getattr(x, 'type', None) == Complete.type:
					self._complete_message = x
		elif issubclass(typ, Exception):
			# Likely raises. CopyManager should trap.
			self.statement.database._pq_complete()

		return super().__exit__(typ, val, tb)

	def count(self):
		return self._complete_message.extract_count()

	def command(self):
		return self._complete_message.extract_command().decode('ascii')

class CallReceiver(Receiver):
	"""
	Call the given object with a list of COPY lines.
	"""
	_e_factors = ('callable',)
	protocol = PROTOCOL_CHUNKS

	def __init__(self, callable):
		self.callable = callable
		self.lines = None
		super().__init__()

	def transmit(self):
		if self.lines is not None:
			self.callable(self.lines)
		self.lines = None

	def accept(self, lines):
		self.lines = lines

class CopyManager(Element, Iterator):
	"""
	A class for managing COPY operations.

	Connects the producer to the receivers.
	"""
	_e_label = 'COPY'
	_e_factors = ('producer', 'receivers',)

	def _e_metas(self):
		yield None, '[' + self.state + ']'

	@property
	def state(self):
		if self.transformer is None:
			return 'initialized'
		return str(self.producer.total_messages) + ' messages transferred'

	def __init__(self, producer, *receivers):
		self.producer = producer
		self.transformer = None
		self.receivers = ElementSet(receivers)
		self._seen_stop_iteration = False
		rp = set()
		add = rp.add
		for x in self.receivers:
			add(x.protocol)
		self.protocols = rp

	def __enter__(self):
		if self.transformer:
			raise RuntimeError("copy already started")
		self._stats = (0, 0)
		self.transformer = CopyTransformer(self.producer.protocol, self.protocols)
		self.producer.__enter__()
		try:
			for x in self.receivers:
				x.__enter__()
		except Exception:
			self.__exit__(*sys.exc_info())
		return self

	def __exit__(self, typ, val, tb):
		##
		# Exiting the CopyManager is a fairly complex operation.
		#
		# In cases of failure, re-alignment may need to happen
		# for when the receivers are not on a message boundary.
		##
		if typ is not None and not issubclass(typ, Exception):
			# Don't bother, it's an interrupt or sufficient resources.
			return

		profail = None
		try:
			# Does nothing if the COPY was successful.
			self.producer.realign()
			try:
				##
				# If the producer is not aligned to a message boundary,
				# it can emit completion data that will put the receivers
				# back on track.
				# This last service call will move that data onto the receivers.
				self._service_producer()
				##
				# The receivers need to handle any new data in their __exit__.
			except StopIteration:
				# No re-alignment needed.
				pass

			self.producer.__exit__(typ, val, tb)
		except Exception as x:
			# reference profail later.
			profail = x

		# No receivers? It wasn't a success.
		if not self.receivers:
			raise CopyFail(self, "no receivers", producer_fault = profail)

		exit_faults = {}
		for x in self.receivers:
			try:
				x.__exit__(typ, val, tb)
			except Exception as e:
				exit_faults[x] = e

		if typ or exit_faults or profail or not self._seen_stop_iteration:
			raise CopyFail(self,
				"could not complete the COPY operation",
				receiver_faults = exit_faults or None,
				producer_fault = profail
			)

	def reconcile(self, r):
		"""
		Reconcile a receiver that faulted.

		This method should be used to add back a receiver that failed to
		complete its write operation, but is capable of completing the
		operation at this time.
		"""
		if r.protocol not in self.protocols:
			raise RuntimeError("cannot add new receivers to copy operations")
		r.transmit()
		# Okay, add it back.
		self.receivers.add(r)

	def _service_producer(self):
		# Setup current data.
		if not self.receivers:
			# No receivers to take the data.
			raise StopIteration

		try:
			nextdata = next(self.producer)
		except StopIteration:
			# Should be over.
			self._seen_stop_iteration = True
			raise
		except Exception:
			raise ProducerFault(self)

		self.transformer(nextdata)

		# Distribute data to receivers.
		for x in self.receivers:
			x.accept(self.transformer.get(x.protocol))

	def _service_receivers(self):
		faults = {}
		for x in self.receivers:
			# Process all the receivers.
			try:
				x.transmit()
			except Exception as e:
				faults[x] = e
		if faults:
			# The CopyManager is eager to continue the operation.
			for x in faults:
				self.receivers.discard(x)
			raise ReceiverFault(self, faults)

	# Run the COPY to completion.
	def run(self):
		with self:
			try:
				while True:
					self._service_producer()
					self._service_receivers()
			except StopIteration:
				# It's done.
				pass

	def __iter__(self):
		return self

	def __next__(self):
		messages = self.producer.total_messages
		bytes = self.producer.total_bytes

		self._service_producer()
		# Record the progress in case a receiver faults.
		self._stats = (
			self._stats[0] + (self.producer.total_messages - messages),
			self._stats[1] + (self.producer.total_bytes - bytes),
		)
		self._service_receivers()
		# Return the progress.
		current_stats = self._stats
		self._stats = (0, 0)
		return current_stats

def transfer(producer, *receivers):
	"""
	Perform a COPY operation using the given statements::

		>>> import copyman
		>>> copyman.transfer(src.prepare("COPY table TO STDOUT"), dst.prepare("COPY table FROM STDIN"))
	"""
	cm = CopyManager(
		StatementProducer(producer),
		*[x if isinstance(x, Receiver) else StatementReceiver(x) for x in receivers]
	)
	cm.run()
	return (cm.producer.total_messages, cm.producer.total_bytes)
