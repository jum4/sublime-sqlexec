##
# .protocol.pbuffer
##
"""
Pure Python message buffer implementation.

Given data read from the wire, buffer the data until a complete message has been
received.
"""
__all__ = ['pq_message_stream']

from io import BytesIO
import struct
from .message_types import message_types

xl_unpack = struct.Struct('!xL').unpack_from

class pq_message_stream(object):
	'provide a message stream from a data stream'
	_block = 512
	_limit = _block * 4
	def __init__(self):
		self._strio = BytesIO()
		self._start = 0

	def truncate(self):
		"remove all data in the buffer"
		self._strio.truncate(0)
		self._start = 0

	def _rtruncate(self, amt = None):
		"[internal] remove the given amount of data"
		strio = self._strio
		if amt is None:
			amt = self._strio.tell()
		strio.seek(0, 2)
		size = strio.tell()
		# if the total size is equal to the amt,
		# then the whole thing is going to be truncated.
		if size == amt:
			strio.truncate(0)
			return

		copyto_pos = 0
		copyfrom_pos = amt
		while True:
			strio.seek(copyfrom_pos)
			data = strio.read(self._block)
			# Next copyfrom
			copyfrom_pos = strio.tell()
			strio.seek(copyto_pos)
			strio.write(data)
			if len(data) != self._block:
				break
			# Next copyto
			copyto_pos = strio.tell()

		strio.truncate(size - amt)

	def has_message(self, xl_unpack = xl_unpack, len = len):
		"if the buffer has a message available"
		strio = self._strio
		strio.seek(self._start)
		header = strio.read(5)
		if len(header) < 5:
			return False
		length, = xl_unpack(header)
		if length < 4:
			raise ValueError("invalid message size '%d'" %(length,))
		strio.seek(0, 2)
		return (strio.tell() - self._start) >= length + 1

	def __len__(self, xl_unpack = xl_unpack, len = len):
		"number of messages in buffer"
		count = 0
		rpos = self._start
		strio = self._strio
		strio.seek(self._start)
		while True:
			# get the message metadata
			header = strio.read(5)
			rpos += 5
			if len(header) < 5:
				# not enough data for another message
				break
			# unpack the length from the header
			length, = xl_unpack(header)
			rpos += length - 4

			if length < 4:
				raise ValueError("invalid message size '%d'" %(length,))
			strio.seek(length - 4 - 1, 1)

			if len(strio.read(1)) != 1:
				break
			count += 1
		return count

	def _get_message(self,
		mtypes = message_types,
		len = len,
		xl_unpack = xl_unpack,
	):
		strio = self._strio
		header = strio.read(5)
		if len(header) < 5:
			return
		length, = xl_unpack(header)
		typ = mtypes[header[0]]

		if length < 4:
			raise ValueError("invalid message size '%d'" %(length,))
		length -= 4
		body = strio.read(length)
		if len(body) < length:
			# Not enough data for message.
			return
		return (typ, body)

	def next_message(self):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		self._strio.seek(self._start)
		msg = self._get_message()
		if msg is not None:
			self._start = self._strio.tell()
		return msg

	def __next__(self):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		self._strio.seek(self._start)
		msg = self._get_message()
		if msg is None:
			raise StopIteration
		self._start = self._strio.tell()
		return msg

	def read(self, num = 0xFFFFFFFF, len = len):
		if self._start > self._limit:
			self._rtruncate(self._start)
			self._start = 0

		new_start = self._start
		self._strio.seek(new_start)
		l = []
		while len(l) < num:
			msg = self._get_message()
			if msg is None:
				break
			l.append(msg)
			new_start += (5 + len(msg[1]))
		self._start = new_start
		return l

	def write(self, data):
		# Always append data; it's a stream, damnit..
		self._strio.seek(0, 2)
		self._strio.write(data)

	def getvalue(self):
		self._strio.seek(self._start)
		return self._strio.read()
