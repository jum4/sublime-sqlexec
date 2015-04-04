##
# .test.test_cluster
##
import sys
import os
import time
import unittest
import tempfile
from .. import installation
from ..cluster import Cluster, ClusterStartupError

default_install = installation.default()
if default_install is None:
	sys.stderr.write("ERROR: cannot find 'default' pg_config\n")
	sys.stderr.write("HINT: set the PGINSTALLATION environment variable to the `pg_config` path\n")
	sys.exit(1)

class test_cluster(unittest.TestCase):
	def setUp(self):
		self.cluster = Cluster(default_install, 'test_cluster',)

	def tearDown(self):
		self.cluster.drop()
		self.cluster = None

	def start_cluster(self, logfile = None):
		self.cluster.start(logfile = logfile)
		self.cluster.wait_until_started(timeout = 10)

	def init(self, *args, **kw):
		self.cluster.init(*args, **kw)
		self.cluster.settings.update({
			'max_connections' : '8',
			'listen_addresses' : 'localhost',
			'port' : '6543',
			'unix_socket_directory' : self.cluster.data_directory,
		})

	def testSilentMode(self):
		self.init()
		self.cluster.settings['silent_mode'] = 'on'
		# if it fails to start(ClusterError), silent_mode is not working properly.
		try:
			self.start_cluster(logfile = sys.stdout)
		except ClusterStartupError:
			# silent_mode is not supported on windows by PG.
			if sys.platform in ('win32','win64'):
				pass
			elif self.cluster.installation.version_info[:2] >= (9, 2):
				pass
			else:
				raise
		else:
			if sys.platform in ('win32','win64'):
				self.fail("silent_mode unexpectedly supported on windows")
			elif self.cluster.installation.version_info[:2] >= (9, 2):
				self.fail("silent_mode unexpectedly supported on PostgreSQL >=9.2")

	def testSuperPassword(self):
		self.init(
			user = 'test',
			password = 'secret',
			logfile = sys.stdout,
		)
		self.start_cluster()
		c = self.cluster.connection(
			user='test',
			password='secret',
			database='template1',
		)
		with c:
			self.assertEqual(c.prepare('select 1').first(), 1)

	def testNoParameters(self):
		'simple init and drop'
		self.init()
		self.start_cluster()

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
