##
# .test.test_alock - test .alock
##
import unittest
import threading
import time
from ..temporal import pg_tmp
from .. import alock

n_alocks = "select count(*) FROM pg_locks WHERE locktype = 'advisory'"

class test_alock(unittest.TestCase):
	@pg_tmp
	def testALockWait(self):
		# sadly, this is primarily used to exercise the code paths..
		ad = prepare(n_alocks).first
		self.assertEqual(ad(), 0)
		state = [False, False, False]
		alt = new()
		first = alock.ExclusiveLock(db, (0,0))
		second = alock.ExclusiveLock(db, 1)
		def concurrent_lock():
			try:
				with alock.ExclusiveLock(alt, 1):
					with alock.ExclusiveLock(alt, (0,0)):
						# start it
						state[0] = True
						while not state[1]:
							pass
							time.sleep(0.01)
					while not state[2]:
						time.sleep(0.01)
			except Exception:
				# Avoid dead lock in cases where advisory is not available.
				state[0] = state[1] = state[2] = True
		t = threading.Thread(target = concurrent_lock)
		t.start()
		while not state[0]:
			time.sleep(0.01)
		self.assertEqual(ad(), 2)
		state[1] = True
		with first:
			self.assertEqual(ad(), 2)
			state[2] = True
			with second:
				self.assertEqual(ad(), 2)
		t.join(timeout = 1)

	@pg_tmp
	def testALockNoWait(self):
		alt = new()
		ad = prepare(n_alocks).first
		self.assertEqual(ad(), 0)
		with alock.ExclusiveLock(db, (0,0)):
			l=alock.ExclusiveLock(alt, (0,0))
			# should fail to acquire
			self.assertEqual(l.acquire(blocking=False), False)
		# no alocks should exist now
		self.assertEqual(ad(), 0)

	@pg_tmp
	def testALock(self):
		ad = prepare(n_alocks).first
		self.assertEqual(ad(), 0)
		# test a variety..
		lockids = [
			(1,4),
			-32532, 0, 2,
			(7, -1232),
			4, 5, 232142423,
			(18,7),
			2, (1,4)
		]
		alt = new()
		xal1 = alock.ExclusiveLock(db, *lockids)
		xal2 = alock.ExclusiveLock(db, *lockids)
		sal1 = alock.ShareLock(db, *lockids)
		with sal1:
			with xal1, xal2:
				self.assertTrue(ad() > 0)
				for x in lockids:
					xl = alock.ExclusiveLock(alt, x)
					self.assertEqual(xl.acquire(blocking=False), False)
				# main has exclusives on these, so this should fail.
				xl = alock.ShareLock(alt, *lockids)
				self.assertEqual(xl.acquire(blocking=False), False)
			for x in lockids:
				# sal1 still holds
				xl = alock.ExclusiveLock(alt, x)
				self.assertEqual(xl.acquire(blocking=False), False)
				# sal1 still holds, but we want a share lock too.
				xl = alock.ShareLock(alt, x)
				self.assertEqual(xl.acquire(blocking=False), True)
				xl.release()
		# no alocks should exist now
		self.assertEqual(ad(), 0)

	@pg_tmp
	def testPartialALock(self):
		# Validates that release is properly cleaning up
		ad = prepare(n_alocks).first
		self.assertEqual(ad(), 0)
		held = (0,-1234)
		wanted = [0, 324, -1232948, 7, held, 1, (2,4), (834,1)]
		alt = new()
		with alock.ExclusiveLock(db, held):
			l=alock.ExclusiveLock(alt, *wanted)
			# should fail to acquire, db has held
			self.assertEqual(l.acquire(blocking=False), False)
		# No alocks should exist now.
		# This *MUST* occur prior to alt being closed.
		# Otherwise, we won't be testing for the recovery
		# of a failed non-blocking acquire().
		self.assertEqual(ad(), 0)

	@pg_tmp
	def testALockParameterErrors(self):
		self.assertRaises(TypeError, alock.ALock)
		l = alock.ExclusiveLock(db)
		self.assertRaises(RuntimeError, l.release)

	@pg_tmp
	def testALockOnClosed(self):
		ad = prepare(n_alocks).first
		self.assertEqual(ad(), 0)
		held = (0,-1234)
		alt = new()
		# __exit__ should only touch the count.
		with alock.ExclusiveLock(alt, held) as l:
			self.assertEqual(ad(), 1)
			self.assertEqual(l.locked(), True)
			alt.close()
			time.sleep(0.005)
			self.assertEqual(ad(), 0)
			self.assertEqual(l.locked(), False)

if __name__ == '__main__':
	unittest.main()
