##
# .cluster - PostgreSQL cluster management
##
"""
Create, control, and destroy PostgreSQL clusters.

postgresql.cluster provides a programmer's interface to controlling a PostgreSQL
cluster. It provides direct access to proper signalling interfaces.
"""
import sys
import os
import errno
import time
import subprocess as sp
from tempfile import NamedTemporaryFile

from . import api as pg_api
from . import configfile
from . import installation as pg_inn
from . import exceptions as pg_exc
from . import driver as pg_driver
from .encodings.aliases import get_python_name
from .python.os import close_fds

if sys.platform in ('win32', 'win64'):
	from .port import signal1_msw as signal
	pg_kill = signal.kill
	def namedtemp(encoding):
		return NamedTemporaryFile(delete = False, mode = 'w', encoding=encoding)
else:
	import signal
	pg_kill = os.kill
	def namedtemp(encoding):
		return NamedTemporaryFile(mode = 'w', encoding=encoding)

class ClusterError(pg_exc.Error):
	"""
	General cluster error.
	"""
	code = '-C000'
	source = 'CLUSTER'
class ClusterInitializationError(ClusterError):
	"General cluster initialization failure"
	code = '-Cini'
class InitDBError(ClusterInitializationError):
	"A non-zero result was returned by the initdb command"
	code = '-Cidb'
class ClusterStartupError(ClusterError):
	"Cluster startup failed"
	code = '-Cbot'
class ClusterNotRunningError(ClusterError):
	"Cluster is not running"
	code = '-Cdwn'
class ClusterTimeoutError(ClusterError):
	"Cluster operation timed out"
	code = '-Cout'

class ClusterWarning(pg_exc.Warning):
	"Warning issued by cluster operations"
	code = '-Cwrn'
	source = 'CLUSTER'

DEFAULT_CLUSTER_ENCODING = 'utf-8'
DEFAULT_CONFIG_FILENAME = 'postgresql.conf'
DEFAULT_HBA_FILENAME = 'pg_hba.conf'
DEFAULT_PID_FILENAME = 'postmaster.pid'

initdb_option_map = {
	'encoding' : '-E',
	'authentication' : '-A',
	'user' : '-U',
	# pwprompt is not supported.
	# interactive use should be implemented by the application
	# calling Cluster.init()
}

class Cluster(pg_api.Cluster):
	"""
	Interface to a PostgreSQL cluster.

	Provides mechanisms to start, stop, restart, kill, drop, and initalize a
	cluster(data directory).

	Cluster does not strive to be consistent with ``pg_ctl``. This is considered
	to be a base class for managing a cluster, and is intended to be extended to
	accommodate for a particular purpose.
	"""
	driver = pg_driver.default
	installation = None
	data_directory = None
	DEFAULT_CLUSTER_ENCODING = DEFAULT_CLUSTER_ENCODING
	DEFAULT_CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME
	DEFAULT_PID_FILENAME = DEFAULT_PID_FILENAME
	DEFAULT_HBA_FILENAME = DEFAULT_HBA_FILENAME

	@property
	def state(self):
		if self.running():
			return 'running'
		if not os.path.exists(self.data_directory):
			return 'void'
		return 'stopped'

	def _e_metas(self):
		state = self.state
		yield (None, '[' + state + ']')
		if state == 'running':
			yield ('pid', self.state)

	@property
	def daemon_path(self):
		"""
		Path to the executable to use to startup the cluster.
		"""
		return self.installation.postmaster or self.installation.postgres

	def get_pid_from_file(self):
		"""
		The current pid from the postmaster.pid file.
		"""
		try:
			path = os.path.join(self.data_directory, self.DEFAULT_PID_FILENAME)
			with open(path) as f:
				return int(f.readline())
		except IOError as e:
			if e.errno in (errno.EIO, errno.ENOENT):
				return None

	@property
	def pid(self):
		"""
		If we have the subprocess, use the pid on the object.
		"""
		pid = self.get_pid_from_file()
		if pid is None:
			d = self.daemon_process
			if d is not None:
				return d.pid
		return pid

	@property
	def settings(self):
		if not hasattr(self, '_settings'):
			self._settings = configfile.ConfigFile(self.pgsql_dot_conf)
		return self._settings

	@property
	def hba_file(self, join = os.path.join):
		"""
		The path to the HBA file of the cluster.
		"""
		return self.settings.get(
			'hba_file',
			join(self.data_directory, self.DEFAULT_HBA_FILENAME)
		)

	def __init__(self,
		installation : "installation object",
		data_directory : "path to the data directory",
	):
		self.installation = installation
		self.data_directory = os.path.abspath(data_directory)
		self.pgsql_dot_conf = os.path.join(
			self.data_directory,
			self.DEFAULT_CONFIG_FILENAME
		)
		self.daemon_process = None
		self.daemon_command = None

	def __repr__(self, format = "{mod}.{name}({ins!r}, {dir!r})".format):
		return format(
			type(self).__module__,
			type(self).__name__,
			self.installation,
			self.data_directory,
		)

	def __enter__(self):
		"""
		Start the cluster and wait for it to startup.
		"""
		self.start()
		self.wait_until_started()
		return self

	def __exit__(self, typ, val, tb):
		"""
		Stop the cluster and wait for it to shutdown.
		"""
		self.stop()
		self.wait_until_stopped()

	def init(self,
		password : \
			"Password to assign to the " \
			"cluster's superuser(`user` keyword)." = None,
		**kw
	):
		"""
		Create the cluster at the given `data_directory` using the
		provided keyword parameters as options to the command.

		`command_option_map` provides the mapping of keyword arguments
		to command options.
		"""
		initdb = self.installation.initdb
		if initdb is None:
			initdb = (self.installation.pg_ctl, 'initdb',)
		else:
			initdb = (initdb,)

		if None in initdb:
			raise ClusterInitializationError(
				"unable to find executable for cluster initialization",
				details = {
					'detail' : "The installation does not have 'initdb' or 'pg_ctl'.",
				},
				creator = self
			)
		# Transform keyword options into command options for the executable.

		# A default is used rather than looking at the environment to, well,
		# avoid looking at the environment.
		kw.setdefault('encoding', self.DEFAULT_CLUSTER_ENCODING)
		opts = []
		for x in kw:
			if x in ('logfile', 'extra_arguments'):
				continue
			if x not in initdb_option_map:
				raise TypeError("got an unexpected keyword argument %r" %(x,))
			opts.append(initdb_option_map[x])
			opts.append(kw[x])
		logfile = kw.get('logfile') or sp.PIPE
		extra_args = tuple([
			str(x) for x in kw.get('extra_arguments', ())
		])

		supw_file = ()
		supw_tmp = None
		p = None
		try:
			if password is not None:
				# got a superuserpass, store it in a tempfile for initdb
				supw_tmp = namedtemp(encoding = get_python_name(kw['encoding']))
				supw_tmp.write(password)
				supw_tmp.flush()
				supw_file = ('--pwfile=' + supw_tmp.name,)

			cmd = initdb + ('-D', self.data_directory) \
				+ tuple(opts) \
				+ supw_file \
				+ extra_args

			p = sp.Popen(
				cmd,
				close_fds = close_fds,
				bufsize = 1024 * 5, # not expecting this to ever be filled.
				stdin = sp.PIPE,
				stdout = logfile,
				# stderr is used to identify a reasonable error message.
				stderr = sp.PIPE,
			)
			# stdin is not used; it is not desirable for initdb to be attached.
			p.stdin.close()

			while True:
				try:
					rc = p.wait()
					break
				except OSError as e:
					if e.errno != errno.EINTR:
						raise
				finally:
					if p.stdout is not None:
						p.stdout.close()

			if rc != 0:
				# initdb returned non-zero, pickup stderr and attach to exception.

				r = p.stderr.read().strip()
				try:
					msg = r.decode('utf-8')
				except UnicodeDecodeError:
					# split up the lines, and use rep.
					msg = os.linesep.join([
						repr(x)[2:-1] for x in r.splitlines()
					])
				raise InitDBError(
					"initdb exited with non-zero status",
					details = {
						'command': cmd,
						'stderr': msg,
						'stdout': msg,
					},
					creator = self
				)
		finally:
			if p is not None:
				for x in (p.stderr, p.stdin, p.stdout):
					if x is not None:
						x.close()

			if supw_tmp is not None:
				n = supw_tmp.name
				supw_tmp.close()
				# XXX: win32 compensation.
				if os.path.exists(n):
					os.unlink(n)

	def drop(self):
		"""
		Stop the cluster and remove it from the filesystem
		"""
		if self.running():
			self.shutdown()
			try:
				self.wait_until_stopped()
			except ClusterTimeoutError:
				self.kill()
				try:
					self.wait_until_stopped()
				except ClusterTimeoutError:
					ClusterWarning(
						'cluster failed to shutdown after kill',
						details = {'hint' : 'Shared memory may have been leaked.'},
						creator = self
					).emit()
		# Really, using rm -rf would be the best, but use this for portability.
		for root, dirs, files in os.walk(self.data_directory, topdown = False):
			for name in files:
				os.remove(os.path.join(root, name))
			for name in dirs:
				os.rmdir(os.path.join(root, name))	
		os.rmdir(self.data_directory)

	def start(self,
		logfile : "Where to send stderr" = None,
		settings : "Mapping of runtime parameters" = None
	):
		"""
		Start the cluster.
		"""
		if self.running():
			return
		cmd = (self.daemon_path, '-D', self.data_directory)
		if settings is not None:
			for k,v in dict(settings).items():
				cmd.append('--{k}={v}'.format(k=k,v=v))

		p = sp.Popen(
			cmd,
			close_fds = close_fds,
			bufsize = 1024,
			# send everything to logfile
			stdout = sp.PIPE if logfile is None else logfile,
			stderr = sp.STDOUT,
			stdin = sp.PIPE,
		)
		if logfile is None:
			p.stdout.close()
		p.stdin.close()
		self.daemon_process = p
		self.daemon_command = cmd

	def restart(self, logfile = None, settings = None, timeout = 10):
		"""
		Restart the cluster gracefully.

		This provides a higher level interface to stopping then starting the
		cluster. It will perform the wait operations and block until the
		restart is complete.

		If waiting is not desired, .start() and .stop() should be used directly.
		"""
		if self.running():
			self.stop()
			self.wait_until_stopped(timeout = timeout)
		if self.running():
			raise ClusterError(
				"failed to shutdown cluster",
				creator = self
			)
		self.start(logfile = logfile, settings = settings)
		self.wait_until_started(timeout = timeout)

	def reload(self):
		"""
		Signal the cluster to reload its configuration file.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGHUP)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def stop(self):
		"""
		Stop the cluster gracefully waiting for clients to disconnect(SIGTERM).
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGTERM)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def shutdown(self):
		"""
		Shutdown the cluster as soon as possible, disconnecting clients.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGINT)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise

	def kill(self):
		"""
		Stop the cluster immediately(SIGKILL).

		Does *not* wait for shutdown.
		"""
		pid = self.pid
		if pid is not None:
			try:
				pg_kill(pid, signal.SIGKILL)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise
				# already dead, so it would seem.

	def initialized(self):
		"""
		Whether or not the data directory *appears* to be a valid cluster.
		"""
		if os.path.isdir(self.data_directory) and \
		os.path.exists(self.pgsql_dot_conf) and \
		os.path.isdir(os.path.join(self.data_directory, 'base')):
			return True
		return False

	def running(self):
		"""
		Whether or not the postmaster is running.

		This does *not* mean the cluster is accepting connections.
		"""
		if self.daemon_process is not None:
			r = self.daemon_process.poll()
			if r is not None:
				pid = self.get_pid_from_file()
				if pid is not None:
					# daemon process does not exist, but there's a pidfile.
					self.daemon_process = None
					return self.running()
				return False
			else:
				return True
		else:
			pid = self.get_pid_from_file()
			if pid is None:
				return False
			try:
				pg_kill(pid, signal.SIG_DFL)
			except OSError as e:
				if e.errno != errno.ESRCH:
					raise
				return False
			return True

	def connector(self, **kw):
		"""
		Create a postgresql.driver connector based on the given keywords and
		listen_addresses and port configuration in settings.
		"""
		host, port = self.address()
		return self.driver.fit(
			host = host or 'localhost',
			port = port or 5432,
			**kw
		)

	def connection(self, **kw):
		"""
		Create a connection object to the cluster, but do not connect.
		"""
		return self.connector(**kw)()

	def connect(self, **kw):
		"""
		Create an established connection from the connector.

		Cluster must be running.
		"""
		if not self.running():
			raise ClusterNotRunningError(
				"cannot connect if cluster is not running",
				creator = self
			)
		x = self.connection(**kw)
		x.connect()
		return x

	def address(self):
		"""
		Get the host-port pair from the configuration.
		"""
		d = self.settings.getset((
			'listen_addresses', 'port',
		))
		if d.get('listen_addresses') is not None:
			# Prefer localhost over other addresses.
			# More likely to get a successful connection.
			addrs = d.get('listen_addresses').lower().split(',')
			if 'localhost' in addrs or '*' in addrs:
				host = 'localhost'
			elif '127.0.0.1' in addrs:
				host = '127.0.0.1'
			elif '::1' in addrs:
				host = '::1'
			else:
				host = addrs[0]
		else:
			host = None
		return (host, d.get('port'))

	def ready_for_connections(self):
		"""
		If the daemon is running, and is not in startup mode.

		This only works for clusters configured for TCP/IP connections.
		"""
		if not self.running():
			return False
		e = None
		host, port = self.address()
		connection = self.driver.fit(
			user = ' -*- ping -*- ',
			host = host, port = port,
			database = 'template1',
			sslmode = 'disable',
		)()
		try:
			connection.connect()
		except pg_exc.ClientCannotConnectError as err:
			for attempt in err.database.failures:
				x = attempt.error
				if self.installation.version_info[:2] < (8,1):
					if isinstance(x, (
						pg_exc.UndefinedObjectError,
						pg_exc.AuthenticationSpecificationError,
					)):
						# undefined user.. whatever...
						return True
				else:
					if isinstance(x, pg_exc.AuthenticationSpecificationError):
						return True
				# configuration file error. ya, that's probably not going to change.
				if isinstance(x, (pg_exc.CFError, pg_exc.ProtocolError)):
					raise x
				if isinstance(x, pg_exc.ServerNotReadyError):
					e = x
					break
			else:
				e = err
		# the else true means we successfully connected with those
		# credentials... strange, but true..
		return e if e is not None else True

	def wait_until_started(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.05,
	):
		"""
		After the `start` method is used, this can be ran in order to block
		until the cluster is ready for use.

		This method loops until `ready_for_connections` returns `True` in
		order to make sure that the cluster is actually up.
		"""
		start = time.time()
		checkpoint = start
		while True:
			if not self.running():
				if self.daemon_process is not None:
					r = self.daemon_process.returncode
					if r is not None:
						raise ClusterStartupError(
							"postgres daemon terminated",
							details = {
								'RESULT' : r,
								'COMMAND' : self.daemon_command,
							},
							creator = self
						)
				else:
					raise ClusterNotRunningError(
						"postgres daemon has not been started",
						creator = self
					)
			r = self.ready_for_connections()

			checkpoint = time.time()
			if r is True:
				break

			if checkpoint - start >= timeout:
				# timeout was reached, but raise ServerNotReadyError
				# to signal to the user that it was *not* due to some unknown
				# condition, rather it's *still* starting up.
				if r is not None and isinstance(r, pg_exc.ServerNotReadyError):
					raise r
				e = ClusterTimeoutError(
					'timeout on startup',
					creator = self
				)
				if r not in (True,False):
					raise e from r
				raise e
			time.sleep(delay)

	def wait_until_stopped(self,
		timeout : "how long to wait before throwing a timeout exception" = 10,
		delay : "how long to sleep before re-testing" = 0.05
	):
		"""
		After the `stop` method is used, this can be ran in order to block until
		the cluster is shutdown.

		Additionally, catching `ClusterTimeoutError` exceptions would be a
		starting point for making decisions about whether or not to issue a kill
		to the daemon.
		"""
		start = time.time()
		while self.running() is True:
			# pickup the exit code.
			if self.daemon_process is not None:
				self.last_exit_code = self.daemon_process.poll()
			else:
				self.last_exit_code = pg_kill(self.get_pid_from_file(), 0)
			if time.time() - start >= timeout:
				raise ClusterTimeoutError(
					'timeout on shutdown',
					creator = self,
				)
			time.sleep(delay)
##
# vim: ts=3:sw=3:noet:
