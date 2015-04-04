##
# .clientparameters
##
"""
Collect client connection parameters from various sources.

This module provides functions for collecting client parameters from various
sources such as user relative defaults, environment variables, and even command
line options.

There are two primary data-structures that this module deals with: normalized
parameters and denormalized parameters.

Normalized parameters is a proper mapping object, dictionary, consisting of
the parameters used to apply to a connection creation interface. The high-level
interface, ``collect`` returns normalized parameters.

Denormalized parameters is a sequence or iterable of key-value pairs. However,
the key is always a tuple whose components make up the "key-path". This is used
to support sub-dictionaries like settings::

	>>> normal_params = {
	 'user' : 'jwp',
	 'host' : 'localhost',
	 'settings' : {'default_statistics_target' : 200, 'search_path' : 'home,public'}
	}

Denormalized parameters are used to simplify the overriding of past parameters.
For this to work with dictionaries in a general fashion, dictionary objects
would need a "deep update" method.
"""
import sys
import os
import configparser
import optparse
from itertools import chain
from functools import partial

from . import iri as pg_iri
from . import pgpassfile as pg_pass
from . exceptions import Error

class ClientParameterError(Error):
	code = '-*000'
	source = '.clientparameters'
class ServiceDoesNotExistError(ClientParameterError):
	code = '-*srv'

try:
	from getpass import getuser, getpass
except ImportError:
	getpass = raw_input
	def getuser():
		return 'postgres'

default_host = 'localhost'
default_port = 5432

pg_service_envvar = 'PGSERVICE'
pg_service_file_envvar = 'PGSERVICEFILE'
pg_sysconfdir_envvar = 'PGSYSCONFDIR'
pg_service_filename = 'pg_service.conf'
pg_service_user_filename = '.pg_service.conf'

# posix
pg_home_passfile = '.pgpass'
pg_home_directory = '.postgresql'

# win32
pg_appdata_directory = 'postgresql'
pg_appdata_passfile = 'pgpass.conf'

# In order to support pg_service.conf, it is
# necessary to identify driver parameters, so
# that database configuration parameters can
# be placed in settings.
pg_service_driver_parameters = set([
	'user',
	'host',
	'database',
	'port',
	'password',

	'sslcrtfile',
	'sslkeyfile',
	'sslrootcrtfile',
	'sslrootkeyfile',

	'sslmode',
	'server_encoding',
	'connect_timeout',
])

# environment variables that will be in the parameters' "settings" dictionary.
default_envvar_settings_map = {
	'TZ' : 'timezone',
	'DATESTYLE' : 'datestyle',
	'CLIENTENCODING' : 'client_encoding',
	'GEQO' : 'geqo',
	'OPTIONS' : 'options',
}

# Environment variables that require no transformation.
default_envvar_map = {
	'USER' : 'user',
	'DATABASE' : 'database',
	'HOST' : 'host',
	'PORT' : 'port',
	'PASSWORD' : 'password',
	'SSLMODE' : 'sslmode',
	'SSLKEY' : 'sslkey',
	'CONNECT_TIMEOUT' : 'connect_timeout',

	'REALM' : 'kerberos4_realm',
	'KRBSRVNAME' : 'kerberos5_service',

	# Extensions
	#'ROLE' : 'role', # SET ROLE $PGROLE

	# This keyword *should* never make it to a connect() function
	# as `resolve_password` should be called to fill in the
	# parameter accordingly.
	'PASSFILE' : 'pgpassfile',
}

def defaults(environ = os.environ):
	"""
	Produce the defaults based on the existing configuration.
	"""
	user = getuser() or 'postgres'
	userdir = os.path.expanduser('~' + user) or '/dev/null'
	pgdata = os.path.join(userdir, pg_home_directory)
	yield ('user',), getuser()
	yield ('host',), default_host
	yield ('port',), default_port

	# If appdata is available, override the pgdata and pgpassfile
	# configuration settings.
	if sys.platform == 'win32':
		appdata = environ.get('APPDATA')
		if appdata:
			pgdata = os.path.join(appdata, pg_appdata_directory)
			pgpassfile = os.path.join(pgdata, pg_appdata_passfile)
	else:
		pgpassfile = os.path.join(userdir, pg_home_passfile)

	for k, v in (
		('sslcrtfile', os.path.join(pgdata, 'postgresql.crt')),
		('sslkeyfile', os.path.join(pgdata, 'postgresql.key')),
		('sslrootcrtfile', os.path.join(pgdata, 'root.crt')),
		('sslrootcrlfile', os.path.join(pgdata, 'root.crl')),
		('pgpassfile', pgpassfile),
	):
		if os.path.exists(v):
			yield (k,), v

def envvars(environ = os.environ, modifier : "environment variable key modifier" = 'PG'.__add__):
	"""
	Create a clientparams dictionary from the given environment variables.

		PGUSER -> user
		PGDATABASE -> database
		PGHOST -> host
		PGHOSTADDR -> host (overrides PGHOST)
		PGPORT -> port

		PGPASSWORD -> password
		PGPASSFILE -> pgpassfile

		PGSSLMODE -> sslmode
		PGREQUIRESSL gets rewritten into "sslmode = 'require'".

		PGREALM -> kerberos4_realm
		PGKRBSVRNAME -> kerberos5_service
		PGSSLKEY -> sslkey

		PGTZ -> settings['timezone']
		PGDATESTYLE -> settings['datestyle']
		PGCLIENTENCODING -> settings['client_encoding']
		PGGEQO -> settings['geqo']

	The 'PG' prefix can be customized via the `modifier` argument. However,
	PGSYSCONFDIR will not respect any such change as it's not a client parameter
	itself.
	"""
	hostaddr = modifier('HOSTADDR')
	reqssl = modifier('REQUIRESSL')
	if reqssl in environ:
		if environ[reqssl].strip() == '1':
			yield ('sslmode',), ('require', reqssl + '=1')

	for k, v in default_envvar_map.items():
		k = modifier(k)
		if k in environ:
			yield ((v,), environ[k])
	if hostaddr in environ:
		yield (('host',), environ[hostaddr])

	envvar_settings_map = ((
		(modifier(k), v) for k,v in default_envvar_settings_map.items()
	))
	settings = [
		(('settings', v,), environ[k]) for k, v in envvar_settings_map if k in environ
	]

	# PGSYSCONFDIR based
	if pg_sysconfdir_envvar in environ:
		yield ('config-pg_sysconfdir', environ[pg_sysconfdir_envvar])
	# PGSERVICEFILE based
	if pg_service_file_envvar in environ:
		yield ('config-pg_service_file', environ[pg_service_file_envvar])

	service = modifier('SERVICE')
	if service in environ:
		yield ('pg_service', environ[service])

##
# optparse options
##

option_datadir = optparse.make_option('-D', '--datadir',
	help = 'location of the database storage area',
	default = None,
	dest = 'datadir',
)

option_in_xact = optparse.make_option('-1', '--with-transaction',
	dest = 'in_xact',
	action = 'store_true',
	help = 'run operation with a transaction block',
)

def append_db_client_parameters(option, opt_str, value, parser):
	# for options without arguments, None is passed in.
	value = True if value is None else value
	parser.values.db_client_parameters.append(
		((option.dest,), value)
	)

make_option = partial(
	optparse.make_option,
	action = 'callback',
	callback = append_db_client_parameters
)

option_user = make_option('-U', '--username',
	dest = 'user',
	type = 'str',
	help = 'user name to connect as',
)
option_database = make_option('-d', '--database',
	type = 'str',
	help = "database's name",
	dest = 'database',
)
option_password = make_option('-W', '--password',
	dest = 'prompt_password',
	help = 'prompt for password',
)
option_host = make_option('-h', '--host',
	help = 'database server host',
	type = 'str',
	dest = 'host',
)
option_port = make_option('-p', '--port',
	help = 'database server port',
	type = 'str',
	dest = 'port',
)
option_unix = make_option('--unix',
	help = 'path to filesystem socket',
	type = 'str',
	dest = 'unix',
)

def append_settings(option, opt_str, value, parser):
	'split the string into a (key,value) pair tuple'
	kv = value.split('=', 1)
	if len(kv) != 2:
		raise OptionValueError("invalid setting argument, %r" %(value,))
	parser.values.db_client_parameters.append(
		((option.dest, kv[0]), kv[1])
	)

option_settings = make_option('-s', '--setting',
	dest = 'settings',
	help = 'run-time parameters to set upon connecting',
	callback = append_settings,
	type = 'str',
)

option_sslmode = make_option('--ssl-mode',
	dest = 'sslmode',
	help = 'SSL requirement for connectivity: require, prefer, allow, disable',
	choices = ('require','prefer','allow','disable'),
	type = 'choice',
)

def append_db_client_x_parameters(option, opt_str, value, parser):
	parser.values.db_client_parameters.append((option.dest, value))
make_x_option = partial(make_option, callback = append_db_client_x_parameters)

option_iri = make_x_option('-I', '--iri',
	help = 'database locator string [pq://user:password@host:port/database?[driver_param]=value&setting=value]',
	type = 'str',
	dest = 'pq_iri',
)

option_lib = optparse.make_option('-l',
	help = 'bind the library found in postgresql.sys.libpath to the connection',
	type = 'str',
	dest = 'lib',
	action = 'append'
)
option_libpath = optparse.make_option('-L',
	help = 'append the library path',
	type = 'str',
	dest = 'libpath',
	action = 'append'
)

# PostgreSQL Standard Options
standard_optparse_options = (
	option_host, option_port,
	option_user, option_password,
	option_database,
)

class StandardParser(optparse.OptionParser):
	"""
	Option parser limited to the basic -U, -h, -p, -W, and -D options.
	This parser subclass is necessary for two reasons:

	 1. _add_help_option override to not conflict with -h
	 2. Initialize the db_client_parameters on the parser's values.

	See the DefaultParser for more fun.
	"""
	standard_option_list = standard_optparse_options

	def get_default_values(self, *args, **kw):
		v = super().get_default_values(*args, **kw)
		v.db_client_parameters = []
		return v

	def _add_help_option(self):
		# Only allow long --help so that it will not conflict with -h(host)
		self.add_option("--help",
			action = "help",
			help = "show this help message and exit",
		)

# Extended Options
default_optparse_options = [
	option_unix,
	option_sslmode,
	option_settings,
# Complex Options
	option_iri,
]
default_optparse_options.extend(standard_optparse_options)

class DefaultParser(StandardParser):
	"""
	Parser that includes a variety of connectivity options.
	(IRI, sslmode, settings)
	"""
	standard_option_list = default_optparse_options

def resolve_password(
	parameters : "a fully normalized set of client parameters(dict)",
	getpass = getpass,
	prompt_title = '',
):
	"""
	Given a parameters dictionary, resolve the 'password' key.

	If `prompt_password` is `True`.
	 If sys.stdin is a TTY, use `getpass` to prompt the user.
	 Otherwise, read a single line from sys.stdin.
	 delete 'prompt_password' from the dictionary.

	Otherwise.
	 If the 'password' key is `None`, attempt to resolve the password using the
	 'pgpassfile' key.

	Finally, remove the pgpassfile key as the password has been resolved for the
	given parameters.
	"""
	prompt_for_password = parameters.pop('prompt_password', False)
	pgpassfile = parameters.pop('pgpassfile', None)
	prompt_title = parameters.pop('prompt_title', None)
	if prompt_for_password is True:
		# it's a prompt
		if sys.stdin.isatty():
			prompt = prompt_title or parameters.pop('prompt_title', '')
			prompt += '[' + pg_iri.serialize(parameters, obscure_password = True) + ']'
			parameters['password'] = getpass("Password for " + prompt +": ")
		else:
			# getpass will throw an exception if it's not a tty,
			# so just take the next line.
			pw = sys.stdin.readline()
			# try to clean it up..
			if pw.endswith(os.linesep):
				pw = pw[:len(pw)-len(os.linesep)]
			parameters['password'] = pw
	else:
		if parameters.get('password') is None:
			# No password? Look in the pgpassfile.
			if pgpassfile is not None:
				parameters['password'] = pg_pass.lookup_pgpass(parameters, pgpassfile)
	# Don't need the pgpassfile parameter anymore as the password
	# has been resolved.

def x_settings(sdict, config):
	d=dict(sdict)
	for (k,v) in d.items():
		yield (('settings', k), v)

def denormalize_parameters(p):
	"""
	Given a fully normalized parameters dictionary:
	{'host': 'localhost', 'settings' : {'timezone':'utc'}}

	Denormalize it:
	[(('host',), 'localhost'), (('settings','timezone'), 'utc')]
	"""
	for k,v in p.items():
		if k == 'settings':
			for sk, sv in dict(v).items():
				yield (('settings', sk), sv)
		else:
			yield ((k,), v)

def x_pq_iri(iri, config):
	return denormalize_parameters(pg_iri.parse(iri))

# Lookup service data using the `service_name`
# Be sure to map 'dbname' to 'database'.
def x_pg_service(service_name, config):
	service_files = []

	f = config.get('pg_service_file')
	if f is not None:
		# service file override
		service_files.append(f)
	else:
		# override is not specified, use the user service file
		home = os.path.expanduser('~' + getuser())
		service_files.append(os.path.join(home, pg_service_user_filename))

	# global service file is checked next.
	sysconfdir = config.get('pg_sysconfdir')
	if sysconfdir:
		sf = config.get('pg_service_filename', pg_service_filename)
		f = os.path.join(sysconfdir, sf)
		# existence will be checked later.
		service_files.append(f)

	for sf in service_files:
		if not os.path.exists(sf):
			continue

		cp = configparser.RawConfigParser()
		cp.read(sf)
		try:
			s = cp.items(service_name)
		except configparser.NoSectionError:
			continue

		for (k, v) in s:
			k = k.lower()
			if k == 'ldap':
				yield ('pg_ldap', ':'.join((k, v)))
			elif k == 'pg_service':
				# ignore
				pass
			elif k == 'hostaddr':
				# XXX: should yield ipv as well?
				yield (('host',), v)
			elif k == 'dbname':
				yield (('database',), v)
			elif k not in pg_service_driver_parameters:
				# it's a GUC.
				yield (('settings', k), v)
			else:
				yield ((k,), v)
		else:
			break
	else:
		# iterator exhausted; service not found
		if sum([os.path.exists(x) for x in service_files]):
			details = {
				'context': ', '.join(service_files),
			}
		else:
			details = {
				'hint': "No service files could be found."
			}
		raise ServiceDoesNotExistError(
			'cannot find service named "{0}"'.format(service_name),
			details = details
		)

def x_pg_ldap(ldap_url, config):
	raise NotImplementedError("cannot resolve ldap URLs: " + str(ldap_url))

default_x_callbacks = {
	'settings' : x_settings,
	'pq_iri' : x_pq_iri,
	'pg_service' : x_pg_service,
	'pg_ldap' : x_pg_ldap,
}

def extrapolate(iter, config = None, callbacks = default_x_callbacks):
	"""
	Given an iterable of standardized settings,

		[((path0, path1, ..., pathN), value)]

	Process any callbacks.
	"""
	config = config or {}
	for item in iter:
		k = item[0]
		if isinstance(k, str):
			if k.startswith('config-'):
				config[k[len('config-'):]] = item[1]
			else:
				cb = callbacks.get(k)
				if cb:
					for x in extrapolate(
						cb(item[1], config),
						config = config,
						callbacks = callbacks
					):
						yield x
				else:
					pass
		else:
			yield item

def normalize_parameter(kv):
	"""
	Translate a parameter into standard form.
	"""
	(k, v) = kv
	if k[0] == 'requiressl' and v in ('1', True):
		k[0] = 'sslmode'
		v = 'require'
	elif k[0] == 'dbname':
		k[0] = 'database'
	elif k[0] == 'sslmode':
		v = v.lower()
	return (tuple(k),v)

def normalize(iter):
	"""
	Normally takes the output of `extrapolate` and makes a dictionary suitable
	for applying to a connector.
	"""
	rd = {}
	for (k, v) in iter:
		sd = rd
		for sk in k[:len(k)-1]:
			sd = sd.setdefault(sk, {})
		sd[k[-1]] = v
	return rd

def resolve_pg_service_file(
	environ = os.environ,
	default_pg_sysconfdir = None,
	default_pg_service_filename = pg_service_filename 
):
	sysconfdir = environ.get(pg_sysconfdir_envvar, default_pg_sysconfdir)
	if sysconfdir:
		return os.path.join(sysconfdir, default_pg_service_filename)
	return None

def collect(
	parsed_options : "options parsed using the `DefaultParser`" = None,
	no_defaults : "Don't build-out defaults like 'user' from getpass.getuser()" = False,
	environ : "environment variables to use, `None` to disable" = os.environ,
	environ_prefix : "prefix to use for collecting environment variables" = 'PG',
	default_pg_sysconfdir : "default 'PGSYSCONFDIR' to use" = None,
	pg_service_file : "the pg-service file to actually use" = None,
	prompt_title : "additional title to use if a prompt request is made" = '',
	parameters : "base-client parameters to use(applied after defaults)" = (),
):
	"""
	Build a normalized client parameters dictionary for use with a connection
	construction interface.
	"""
	d_parameters = []
	d_parameters.append([('config-environ', environ)])
	if default_pg_sysconfdir is not None:
		d_parameters.append([
			('config-pg_sysconfdir', default_pg_sysconfdir)
		])
	if pg_service_file is not None:
		d_parameters.append([
			('config-pg_service_file', pg_service_file)
		])

	if not no_defaults:
		d_parameters.append(defaults(environ = environ))

	if parameters:
		d_parameters.append(denormalize_parameters(dict(parameters)))

	if environ is not None:
		d_parameters.append(envvars(
			environ = environ,
			modifier = environ_prefix.__add__
		))
	cop = getattr(parsed_options, 'db_client_parameters', None)
	if cop:
		d_parameters.append(cop)

	cpd = normalize(extrapolate(chain(*d_parameters)))
	if prompt_title is not None:
		resolve_password(cpd, prompt_title = prompt_title)
	return cpd

if __name__ == '__main__':
	import pprint
	p = DefaultParser(
		description = "print the clientparams dictionary for the environment"
	)
	(co, ca) = p.parse_args()
	r = collect(parsed_options = co, prompt_title = 'custom_prompt_title')
	pprint.pprint(r)
