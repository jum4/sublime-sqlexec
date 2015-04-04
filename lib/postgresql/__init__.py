##
# py-postgresql root package
# http://python.projects.postgresql.org
##
"""
py-postgresql is a Python package for using PostgreSQL. This includes low-level
protocol tools, a driver(PG-API and DB-API), and cluster management tools.

If it's not documented in the narratives, `postgresql.documentation.index`, then
the stability of the APIs should *not* be trusted.

See <http://postgresql.org> for more information about PostgreSQL.
"""
__all__ = [
	'__author__',
	'__date__',
	'__version__',
	'__docformat__',
	'version',
	'version_info',
	'open',
]

#: The version string of py-postgresql.
version = '' # overridden by subsequent import from .project.

#: The version triple of py-postgresql: (major, minor, patch).
version_info = () # overridden by subsequent import from .project.

# Optional.
try:
	from .project import version_info, version, \
		author as __author__, date as __date__
	__version__ = version
except ImportError:
	pass

# Avoid importing these until requested.
_pg_iri = _pg_driver = _pg_param = None
def open(iri = None, prompt_title = None, **kw):
	"""
	Create a `postgresql.api.Connection` to the server referenced by the given
	`iri`::

		>>> import postgresql
		# General Format:
		>>> db = postgresql.open('pq://user:password@host:port/database')

		# Connect to 'postgres' at localhost.
		>>> db = postgresql.open('localhost/postgres')

	Connection keywords can also be used with `open`. See the narratives for
	more information.

	The `prompt_title` keyword is ignored. `open` will never prompt for
	the password unless it is explicitly instructed to do so.

	(Note: "pq" is the name of the protocol used to communicate with PostgreSQL)
	"""
	global _pg_iri, _pg_driver, _pg_param
	if _pg_iri is None:
		from . import iri as _pg_iri
		from . import driver as _pg_driver
		from . import clientparameters as _pg_param

	return_connector = False
	if iri is not None:
		if iri.startswith('&'):
			return_connector = True
			iri = iri[1:]
		iri_params = _pg_iri.parse(iri)
		iri_params.pop('path', None)
	else:
		iri_params = {}

	std_params = _pg_param.collect(prompt_title = None)
	# If unix is specified, it's going to conflict with any standard
	# settings, so remove them right here.
	if 'unix' in kw or 'unix' in iri_params:
		std_params.pop('host', None)
		std_params.pop('port', None)
	params = _pg_param.normalize(
		list(_pg_param.denormalize_parameters(std_params)) + \
		list(_pg_param.denormalize_parameters(iri_params)) + \
		list(_pg_param.denormalize_parameters(kw))
	)
	_pg_param.resolve_password(params)

	C = _pg_driver.default.fit(**params)
	if return_connector is True:
		return C
	else:
		c = C()
		c.connect()
		return c

__docformat__ = 'reStructuredText'
