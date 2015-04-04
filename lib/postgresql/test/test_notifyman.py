##
# .test.test_notifyman - test .notifyman
##
import unittest
import threading
import time
from ..temporal import pg_tmp
from ..notifyman import NotificationManager

class test_notifyman(unittest.TestCase):
	@pg_tmp
	def testNotificationManager(self):
		# signals each other
		alt = new()
		with alt:
			nm = NotificationManager(db, alt)
			db.listen('foo')
			alt.listen('bar')
			# notify the other.
			alt.notify('foo')
			db.notify('bar')
			# we can separate these here because there's no timeout
			for ndb, notifies in nm:
				for n in notifies:
					if ndb is db:
						self.assertEqual(n[0], 'foo')
						self.assertEqual(n[1], '')
						self.assertEqual(n[2], alt.backend_id)
						nm.connections.discard(db)
					elif ndb is alt:
						self.assertEqual(n[0], 'bar')
						self.assertEqual(n[1], '')
						self.assertEqual(n[2], db.backend_id)
						nm.connections.discard(alt)
					else:
						self.fail("unknown connection received notify..")

	@pg_tmp
	def testNotificationManagerTimeout(self):
		nm = NotificationManager(db, timeout = 0.1)
		db.listen('foo')
		count = 0
		for event in nm:
			if event is None:
				# do this a few times, then break out of the loop
				db.notify('foo')
				continue
			ndb, notifies = event
			self.assertEqual(ndb, db)
			for n in notifies:
				self.assertEqual(n[0], 'foo')
				self.assertEqual(n[1], '')
				self.assertEqual(n[2], db.backend_id)
				count = count + 1
			if count > 3:
				break

	@pg_tmp
	def testNotificationManagerZeroTimeout(self):
		# Zero-timeout means raise StopIteration when
		# there are no notifications to emit.
		# It checks the wire, but does *not* wait for data.
		nm = NotificationManager(db, timeout = 0)
		db.listen('foo')
		self.assertEqual(list(nm), [])
		db.notify('foo')
		time.sleep(0.01)
		self.assertEqual(list(nm), [('foo','',db.backend_id)]) # bit of a race

	@pg_tmp
	def test_iternotifies(self):
		# db.iternotifies() simplification of NotificationManager
		alt = new()
		alt.listen('foo')
		alt.listen('close')
		def get_notices(db, l):
			with db:
				for x in db.iternotifies():
					if x[0] == 'close':
						break
					l.append(x)
		rl = []
		t = threading.Thread(target = get_notices, args = (alt, rl,))
		t.start()
		db.notify('foo')
		while not rl:
			time.sleep(0.05)
		channel, payload, pid = rl.pop(0)
		self.assertEqual(channel, 'foo')
		self.assertEqual(payload, '')
		self.assertEqual(pid, db.backend_id)
		db.notify('close')

	@pg_tmp
	def testNotificationManagerZeroTimeout(self):
		# Zero-timeout means raise StopIteration when
		# there are no notifications to emit.
		# It checks the wire, but does *not* wait for data.
		db.listen('foo')
		self.assertEqual(list(db.iternotifies(0)), [])
		db.notify('foo')
		time.sleep(0.01)
		self.assertEqual(list(db.iternotifies(0)), [('foo','', db.backend_id)]) # bit of a race

	@pg_tmp
	def testNotificationManagerOnClosed(self):
		# When the connection goes away, the NM iterator
		# should raise a Stop.
		db = new()
		db.listen('foo')
		db.notify('foo')
		for n in db.iternotifies():
			db.close()
		self.assertEqual(db.closed, True)
		del db
		# closer, after an idle
		db = new()
		db.listen('foo')
		for n in db.iternotifies(0.2):
			if n is None:
				# In the loop, notify, and expect to
				# get the notification even though the
				# connection was closed.
				db.notify('foo')
				db.execute('')
				db.close()
				hit = False
			else:
				hit = True
		# hit should get set two times.
		# once on the first idle, and once on the event
		# received after the close.
		self.assertEqual(db.closed, True)
		self.assertEqual(hit, True)

if __name__ == '__main__':
	unittest.main()
