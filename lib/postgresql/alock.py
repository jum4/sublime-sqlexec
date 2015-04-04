##
# .alock - Advisory Locks
##
"""
Tools for Advisory Locks
"""
from abc import abstractmethod, abstractproperty
from .python.element import Element

__all__ = [
	'ALock',
	'ExclusiveLock',
	'ShareLock',
]

class ALock(Element):
	"""
	Advisory Lock class for managing the acquisition and release of a sequence
	of PostgreSQL advisory locks.

	ALock()'s are fairly consistent with threading.RLock()'s. They can be
	acquired multiple times, and they must be released the same number of times
	for the lock to actually be released.

	A notably difference is that ALock's manage a sequence of lock identifiers.
	This means that a given ALock() may represent multiple advisory locks.
	"""
	_e_factors = ('database', 'identifiers',)
	_e_label = 'ALOCK'
	def _e_metas(self,
		headfmt = "{1} [{0}]".format
	):
		yield None, headfmt(self.state, self.mode)

	@abstractproperty
	def mode(self):
		"""
		The mode of the lock class.
		"""

	@abstractproperty
	def __select_statements__(self):
		"""
		Implemented by subclasses to return the statements to try, acquire, and
		release the advisory lock.

		Returns a triple of callables where each callable takes two arguments,
		the lock-id pairs, and then the int8 lock-ids.
		``(try, acquire, release)``.
		"""

	@staticmethod
	def _split_lock_identifiers(idseq):
		# lame O(2)
		id_pairs = [
			list(x) if x.__class__ is not int else [None,None]
			for x in idseq
		]
		ids = [
			x if x.__class__ is int else None
			for x in idseq
		]
		return (id_pairs, ids)

	def acquire(self, blocking = True, len = len):
		"""
		Acquire the locks using the configured identifiers.
		"""
		if self._count == 0:
			# _count is zero, so the locks need to be acquired.
			wait = bool(blocking)
			if wait:
				self._acquire(self._id_pairs, self._ids)
			else:
				# grab the success of each lock id. if some were
				# unsuccessful, then the ones that were successful need to be
				# released.
				r = self._try(self._id_pairs, self._ids)
				# accumulate the identifiers that *did* lock
				release_seq = [
					id for didlock, id in zip(r, self.identifiers) if didlock[0]
				]
				if len(release_seq) != len(self.identifiers):
					# some failed, so release the acquired and return False
					#
					# reverse in case there is another waiting for all.
					# that is, release last-to-first so that if another is waiting
					# on the same seq that it should be able to acquire all of
					# them once the contended lock is released.
					release_seq.reverse()
					self._release(*self._split_lock_identifiers(release_seq))
					# unable to acquire all.
					return False
		self._count = self._count + 1
		return True

	def __enter__(self):
		self.acquire()
		return self

	def release(self):
		"""
		Release the locks using the configured identifiers.
		"""
		if self._count < 1:
			raise RuntimeError("cannot release un-acquired lock")
		if not self.database.closed and self._count > 0:
			# if the database has been closed, or the count will
			# remain non-zero, there is no need to release.
			self._release(reversed(self._id_pairs), reversed(self._ids))
			# decrement the count nonetheless.
		self._count = self._count - 1

	def __exit__(self, typ, val, tb):
		self.release()

	def locked(self):
		"""
		Whether the locks have been acquired. This method is sensitive to the
		connection's state. If the connection is closed, it will return False.
		"""
		return (self._count > 0) and (not self.database.closed)

	@property
	def state(self):
		return 'locked' if self.locked() else 'unlocked'

	def __init__(self, database, *identifiers):
		"""
		Initialize the lock object to manage a sequence of advisory locks
		for use with the given database.
		"""
		self._count = 0
		self.connection = self.database = database
		self.identifiers = identifiers
		self._id_pairs, self._ids = self._split_lock_identifiers(identifiers)
		self._try, self._acquire, self._release = self.__select_statements__()

class ShareLock(ALock):
	mode = 'share'

	def __select_statements__(self):
		return (
			self.database.sys.try_advisory_shared,
			self.database.sys.acquire_advisory_shared,
			self.database.sys.release_advisory_shared,
		)

class ExclusiveLock(ALock):
	mode = 'exclusive'

	def __select_statements__(self):
		return (
			self.database.sys.try_advisory_exclusive,
			self.database.sys.acquire_advisory_exclusive,
			self.database.sys.release_advisory_exclusive,
		)
