##
# .installation
##
"""
Collect and access PostgreSQL installation information.
"""
import sys
import os
import os.path
import subprocess
import io
import errno
from itertools import cycle, chain
from operator import itemgetter
from .python.os import find_executable, close_fds, platform_exe
from . import versionstring
from . import api as pg_api
from . import string as pg_str

# Get the output from the given command.
# *args are transformed into "long options", '--' + x
def get_command_output(exe, *args):
	pa = list(exe) + [
		'--' + x.strip() for x in args if x is not None
	]
	p = subprocess.Popen(pa,
		close_fds = close_fds,
		stdout = subprocess.PIPE,
		stderr = subprocess.PIPE,
		stdin = subprocess.PIPE,
		shell = False
	)
	p.stdin.close()
	p.stderr.close()
	while True:
		try:
			rv = p.wait()
			break
		except OSError as e:
			if e.errno != errno.EINTR:
				raise
	if rv != 0:
		return None
	with p.stdout, io.TextIOWrapper(p.stdout) as txt:
		return txt.read()

def pg_config_dictionary(*pg_config_path):
	"""
	Create a dictionary of the information available in the given
	pg_config_path. This provides a one-shot solution to fetching information
	from the pg_config binary. Returns a dictionary object.
	"""
	default_output = get_command_output(pg_config_path)
	if default_output is not None:
		d = {}
		for x in default_output.splitlines():
			if not x or x.isspace() or x.find('=') == -1:
				continue
			k, v = x.split('=', 1)
			# keep it semi-consistent with instance
			d[k.lower().strip()] = v.strip()
		return d

	# Support for 8.0 pg_config and earlier.
	# This requires three invocations of pg_config:
	#  First --help, to get the -- options available,
	#  Second, all the -- options except version.
	#  Third, --version as it appears to be exclusive in some cases.
	opt = []
	for l in get_command_output(pg_config_path, 'help').splitlines():
		dash_pos = l.find('--')
		if dash_pos == -1:
			continue
		sp_pos = l.find(' ', dash_pos)
		# the dashes are added by the call command
		opt.append(l[dash_pos+2:sp_pos])
	if 'help' in opt:
		opt.remove('help')
	if 'version' in opt:
		opt.remove('version')

	d=dict(zip(opt, get_command_output(pg_config_path, *opt).splitlines()))
	d['version'] = get_command_output(pg_config_path, 'version').strip()
	return d

##
# Build a key-value pair list of the configure options.
# If the item is quoted, mind the quotes.
def parse_configure_options(confopt, quotes = '\'"', dash_and_quotes = '-\'"'):
	# This is not a robust solution, but it will usually work.
	# Chances are that there is a quote at the beginning of this string.
	# However, in the windows pg_config.exe, this appears to be absent.
	if confopt[0:1] in quotes:
		# quote at the beginning. assume it's used consistently.
		quote = confopt[0:1]
	elif confopt[-1:] in quotes:
		# quote at the end?
		quote = confopt[-1]
	else:
		# fallback to something. :(
		quote = "'"
	##
	# This is using the wrong kind of split, but the pg_config
	# output has been consistent enough for this to work.
	parts = pg_str.split_using(confopt, quote, sep = ' ')
	qq = quote * 2
	for x in parts:
		if qq in x:
			# singularize the quotes
			x = x.replace(qq, quote)
		# remove the quotes around '--' from option.
		# if it splits once, the '1' index will
		# be `True`, indicating that the flag was given, but
		# was not given a value.
		kv = x.strip(dash_and_quotes).split('=', 1) + [True]
		key = kv[0].replace('-','_')
		# Ignore empty keys.
		if key:
			yield (key, kv[1])

def default_pg_config(execname = 'pg_config', envkey = 'PGINSTALLATION'):
	"""
	Get the default `pg_config` executable on the system.

	If 'PGINSTALLATION' is in the environment, use it.
	Otherwise, look through the system's PATH environment.
	"""
	pg_config_path = os.environ.get(envkey)
	if pg_config_path:
		# Trust PGINSTALLATION.
		return platform_exe(pg_config_path)
	return find_executable(execname)

class Installation(pg_api.Installation):
	"""
	Class providing a Python interface to PostgreSQL installation information.
	"""
	version = None
	version_info = None
	type = None
	configure_options = None
	#: The pg_config information dictionary.
	info = None

	pg_executables = (
		'pg_config',
		'psql',
		'initdb',
		'pg_resetxlog',
		'pg_controldata',
		'clusterdb',
		'pg_ctl',
		'pg_dump',
		'pg_dumpall',
		'postgres',
		'postmaster',
		'reindexdb',
		'vacuumdb',
		'ipcclean',
		'createdb',
		'ecpg',
		'createuser',
		'createlang',
		'droplang',
		'dropuser',
		'pg_restore',
	)

	pg_libraries = (
		'libpq',
		'libecpg',
		'libpgtypes',
		'libecpg_compat',
	)

	pg_directories = (
		'bindir',
		'docdir',
		'includedir',
		'pkgincludedir',
		'includedir_server',
		'libdir',
		'pkglibdir',
		'localedir',
		'mandir',
		'sharedir',
		'sysconfdir',
	)

	def _e_metas(self):
		l = list(self.configure_options.items())
		l.sort(key = itemgetter(0))
		yield ('version', self.version)
		if l:
			yield ('configure_options',
				(os.linesep).join((
					k if v is True else k + '=' + v
					for k,v in l
				))
			)

	def __repr__(self, format = "{mod}.{name}({info!r})".format):
		return format(
			mod = type(self).__module__,
			name = type(self).__name__,
			info = self.info
		)

	def __init__(self, info : dict):
		"""
		Initialize the Installation using the given information dictionary.
		"""
		self.info = info
		self.version = self.info["version"]
		self.type, vs = self.version.split()
		self.version_info = versionstring.normalize(versionstring.split(vs))
		self.configure_options = dict(
			parse_configure_options(self.info.get('configure', ''))
		)
		# collect the paths in a dictionary first
		self.paths = dict()

		exists = os.path.exists
		join = os.path.join
		for k in self.pg_directories:
			self.paths[k] = self.info.get(k)

		# find all the PG executables that exist for the installation.
		bindir_path = self.info.get('bindir')
		if bindir_path is None:
			self.paths.update(zip(self.pg_executables, cycle((None,))))
		else:
			for k in self.pg_executables:
				path = platform_exe(join(bindir_path, k))
				if exists(path):
					self.paths[k] = path
				else:
					self.paths[k] = None
		self.__dict__.update(self.paths)

	@property
	def ssl(self):
		"""
		Whether the installation was compiled with SSL support.
		"""
		return 'with_openssl' in self.configure_options

def default(typ = Installation):
	"""
	Get the default Installation.

	Uses default_pg_config() to identify the executable.
	"""
	path = default_pg_config()
	if path is None:
		return None
	return typ(pg_config_dictionary(path))

if __name__ == '__main__':
	if sys.argv[1:]:
		d = pg_config_dictionary(sys.argv[1])
		i = Installation(d)
	else:
		i = default()
	from .python.element import format_element
	print(format_element(i))
