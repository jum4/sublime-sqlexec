##
# .notifyman - Receive and manage NOTIFY events.
##
"""
Notification Management Tools

Primarily this module houses the `NotificationManager` class which provides an
iterator for a NOTIFY event loop against a set of connections.

	>>> import postgresql
	>>> db = postgresql.open(...)
	>>> from postgresql.notifyman import NotificationManager
	>>> nm = NotificationManager(db, timeout = 10) # idle events every 10 seconds
	>>> for x in nm:
	...  if x is None:
	...   # idle event
	...   ...
	...  db, notifies = x
	...  for channel, payload, pid in notifies:
	...   ...
"""
from time import time
from select import select
from itertools import chain

class NotificationManager(object):
	"""
	A class for managing the asynchronous notifications received by a
	set of connections.

	Instances provide the iterator for an event loop that responds to NOTIFYs
	received by the connections being watched. There is no thread safety, so
	when a connection is being managed, it should not be used concurrently in
	other threads while being managed.
	"""
	__slots__ = (
		'connections',
		'garbage',
		'incoming',
		'timeout',
		'_last_time',
		'_pulled',
	)

	def __init__(self, *connections, timeout = None):
		self.settimeout(timeout)
		self.connections = set(connections)
		# Connections that failed.
		self.garbage = set()
		# Used to store NOTIFYs consumed from the connections
		self.incoming = None
		self._last_time = None
		# connection -> sequence of NOTIFYs
		self._pulled = dict()

	# Check the wire *and* wait for new messages.
	def _wait_on_wires(self, time = time, select = select):
		if self.timeout == 0:
			# We're polling.
			max_duration = 0
		else:
			# If timeout is None, we don't issue idle events, but
			# we still cycle in case the timeout is changed.
			if self._last_time is not None:
				max_duration = (self.timeout or 10) - (time() - self._last_time)
				if max_duration < 0:
					max_duration = 0
			else:
				self._last_time = time()
				max_duration = self.timeout or 10

		# Connections already marked as "bad" should not be checked.
		check = self.connections - self.garbage
		for db in check:
			if db.closed:
				self.connections.remove(db)
				self.garbage.add(db)
		check = self.connections - self.garbage

		r, w, x = select(check, (), check, max_duration)
		# Make sure the connection's _notifies get filled.
		for db in r:
			# Collect any pending events.
			try:
				# Even if db is in a failed transaction, this
				# 'null' command will succeed.
				# (only connection failures blow up)
				db.execute('')
			except Exception:
				# failed to collect notifies; put in exception list.
				# It is very unlikely that this is *not* a FATAL error.
				x.append(db)
		self.trash(x)

	def trash(self, connections):
		"""
		Remove the given connections from the set of good connections, and add
		them to the `garbage` set.

		This method can be overridden by subclasses to take a callback approach
		to connection failures.
		"""
		# Identify the bad connections.
		self.garbage.update(connections)
		self.connections.difference_update(connections)

	def queue(self, db, notifies):
		"""
		Queue the notifies for the specified connection. Upon success, the 

		This method can be overridden by subclasses to take a callback approach
		to notification management.
		"""
		l = self._pulled.setdefault(db, list())
		l.extend(notifies)

	# Check the connection's _notifies list; just scan everything.
	def _pull_from_connections(self):
		for db in self.connections:
			if not db._notifies:
				# nothing queued up, look at the next connection
				continue
			# Pull notifies into the NotificationManager
			decode = db.typio.decode
			notifies = [
				(decode(x.channel), decode(x.payload), x.pid)
				for x in db._notifies
			]
			self.queue(db, notifies)
			del db._notifies[:len(notifies)]

	# "Append" the pulled NOTIFYs to the 'incoming' iterator.
	def _queue_next(self):
		new_seqs = []
		for db in self._pulled:
			decode = db.typio.decode
			new_seqs.append((db, self._pulled[db]))

		if new_seqs:
			if self.incoming:
				# Already have incoming; not an expected condition,
				# but let's compensate.
				self.incoming, self._pulled = chain(self.incoming, iter(new_seqs)), {}
			else:
				self.incoming, self._pulled = iter(new_seqs), {}
		elif self.incoming is None:
			# Use this to trigger the StopIteration case of zero-timeout.
			self.incoming, self._pulled = iter(()), {}

	def _timedout(self, time = time):
		# Idles are guaranteed to occur, but make sure that
		# __next__ has a chance to check the connections and the wires.
		now = time()
		if self._last_time is None:
			self._last_time = now
		elif self.timeout and now >= (self._last_time + self.timeout):
			# Set last_time to None in case the timeout is so low
			# that this condition keeps NOTIFYs from being seen.
			self._last_time = None
			# Signal timeout.
			return True
		else:
			# toggle back to None.
			self._last_time = None
		return False

	def settimeout(self, seconds):
		"""
		Set the maximum duration, in seconds, for waiting for NOTIFYs on the
		set of managed connections. The given `seconds` argument can be a number
		or `None`.

		A timeout of `None` means no timeout, and "idle" events will never
		occur.

		A timeout of `0` means to never wait for NOTIFYs. This has the effect of
		a StopIteration being raised by `__next__` when there are no more
		Notifications available for any of the connections in the set. "Idle"
		events will never occur in this situation as well.

		A timeout greater than zero means to emit `None` as "idle" events into
		the loop at the specified interval. Idle events are guaranteed to occur.
		"""
		if seconds is not None and seconds < 0:
			raise ValueError("cannot set timeout less than zero")
		self.timeout = seconds

	def gettimeout(self):
		'Get the timeout.'
		return self.timeout

	def __iter__(self):
		return self

	def __next__(self, time = time):
		checked_wire = True
		# Loop until NOTIFY received or timeout.
		while True:
			if self.incoming is not None:
				try:
					return next(self.incoming)
				except StopIteration:
					# Nothing more in this incoming.
					self.incoming = None
					# Allow a zero timeout to be used to indicate
					# that there are no NOTIFYs to be read.
					# This can be used to poll a set of
					# connections instead of listening.
					if self.timeout == 0 or not self.connections:
						raise

			# timeout happened? yield the "idle" event.
			# This check **must** happen after .incoming is checked.
			# Never emit idle when there are real events.
			if self._timedout():
				return None

			if not checked_wire and self.connections:
				# Nothing queued up, check connections if any.
				self._wait_on_wires()
				checked_wire = True
			else:
				checked_wire = False
			self._pull_from_connections()
			self._queue_next()
