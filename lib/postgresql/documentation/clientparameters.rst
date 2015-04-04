Client Parameters
*****************

.. warning:: **The interfaces dealing with optparse are subject to change in 1.0**.

There are various sources of parameters used by PostgreSQL client applications.
The `postgresql.clientparameters` module provides a means for collecting and
managing those parameters.

Connection creation interfaces in `postgresql.driver` are purposefully simple.
All parameters taken by those interfaces are keywords, and are taken
literally; if a parameter is not given, it will effectively be `None`.
libpq-based drivers tend differ as they inherit some default client parameters
from the environment. Doing this by default is undesirable as it can cause
trivial failures due to unexpected parameter inheritance. However, using these
parameters from the environment and other sources are simply expected in *some*
cases: `postgresql.open`, `postgresql.bin.pg_python`, and other high-level
utilities. The `postgresql.clientparameters` module provides a means to collect
them into one dictionary object for subsequent application to a connection
creation interface.

`postgresql.clientparameters` is primarily useful to script authors that want to
provide an interface consistent with PostgreSQL commands like ``psql``.


Collecting Parameters
=====================

The primary entry points in `postgresql.clientparameters` are
`postgresql.clientparameters.collect` and
`postgresql.clientparameters.resolve_password`.

For most purposes, ``collect`` will suffice. By default, it will prompt for the
password if instructed to(``-W``). Therefore, ``resolve_password`` need not be
used in most cases::

	>>> import sys
	>>> import postgresql.clientparameters as pg_param
	>>> p = pg_param.DefaultParser()
	>>> co, ca = p.parse_args(sys.argv[1:])
	>>> params = pg_param.collect(parsed_options = co)

The `postgresql.clientparameters` module is executable, so you can see the
results of the above snippet by::

	$ python -m postgresql.clientparameters -h localhost -U a_db_user -ssearch_path=public
	{'host': 'localhost',
	 'password': None,
	 'port': 5432,
	 'settings': {'search_path': 'public'},
	 'user': 'a_db_user'}


`postgresql.clientparameters.collect`
--------------------------------------

Build a client parameter dictionary from the environment and parsed command
line options. The following is a list of keyword arguments that ``collect`` will
accept:

 ``parsed_options``
  Options parsed by `postgresql.clientparameters.StandardParser` or
  `postgresql.clientparameters.DefaultParser` instances.

 ``no_defaults``
  When `True`, don't include defaults like ``pgpassfile`` and ``user``.
  Defaults to `False`.

 ``environ``
  Environment variables to extract client parameter variables from.
  Defaults to `os.environ` and expects a `collections.Mapping` interface.

 ``environ_prefix``
  Environment variable prefix to use. Defaults to "PG". This allows the
  collection of non-standard environment variables whose keys are partially
  consistent with the standard variants. e.g. "PG_SRC_USER", "PG_SRC_HOST",
  etc.

 ``default_pg_sysconfdir``
  The location of the pg_service.conf file. The ``PGSYSCONFDIR`` environment
  variable will override this. When a default installation is present,
  ``PGINSTALLATION``, it should be set to this.

 ``pg_service_file``
  Explicit location of the service file. This will override the "sysconfdir"
  based path.

 ``prompt_title``
  Descriptive title to use if a password prompt is needed. `None` to disable
  password resolution entirely. Setting this to `None` will also disable
  pgpassfile lookups, so it is necessary that further processing occurs when
  this is `None`.

 ``parameters``
  Base client parameters to use. These are set after the *defaults* are
  collected. (The defaults that can be disabled by ``no_defaults``).

If ``prompt_title`` is not set to `None`, it will prompt for the password when
instructed to do by the ``prompt_password`` key in the parameters::

	>>> import postgresql.clientparameters as pg_param
	>>> p = pg_param.collect(prompt_title = 'my_prompt!', parameters = {'prompt_password':True})
	Password for my_prompt![pq://jwp@localhost:5432]:
	>>> p
	{'host': 'localhost', 'user': 'jwp', 'password': 'secret', 'port': 5432}

If `None`, it will leave the necessary password resolution information in the
parameters dictionary for ``resolve_password``::

	>>> p = pg_param.collect(prompt_title = None, parameters = {'prompt_password':True})
	>>> p
	{'pgpassfile': '/Users/jwp/.pgpass', 'prompt_password': True, 'host': 'localhost', 'user': 'jwp', 'port': 5432}

Of course, ``'prompt_password'`` is normally specified when ``parsed_options``
received a ``-W`` option from the command line::

	>>> op = pg_param.DefaultParser()
	>>> co, ca = op.parse_args(['-W'])
	>>> p = pg_param.collect(parsed_options = co)
	>>> p=pg_param.collect(parsed_options = co)
	Password for [pq://jwp@localhost:5432]:
	>>> p
	{'host': 'localhost', 'user': 'jwp', 'password': 'secret', 'port': 5432}
	>>>


`postgresql.clientparameters.resolve_password`
----------------------------------------------

Resolve the password for the given client parameters dictionary returned by
``collect``. By default, this function need not be used as ``collect`` will
resolve the password by default. `resolve_password` accepts the following
arguments:

 ``parameters``
  First positional argument. Normalized client parameters dictionary to update
  in-place with the resolved password. If the 'prompt_password' key is in
  ``parameters``, it will prompt regardless(normally comes from ``-W``).

 ``getpass``
  Function to call to prompt for the password. Defaults to `getpass.getpass`.

 ``prompt_title``
  Additional title to use if a prompt is requested. This can also be specified
  in the ``parameters`` as the ``prompt_title`` key. This *augments* the IRI
  display on the prompt. Defaults to an empty string, ``''``.

The resolution process is effected by the contents of the given ``parameters``.
Notable keywords:

 ``prompt_password``
  If present in the given parameters, the user will be prompted for the using
  the given ``getpass`` function. This disables the password file lookup
  process.

 ``prompt_title``
  This states a default prompt title to use. If the ``prompt_title`` keyword
  argument is given to ``resolve_password``, this will not be used.

 ``pgpassfile``
  The PostgreSQL password file to lookup the password in. If the ``password``
  parameter is present, this will not be used.

When resolution occurs, the ``prompt_password``, ``prompt_title``, and
``pgpassfile`` keys are *removed* from the given parameters dictionary::

	>>> p=pg_param.collect(prompt_title = None)
	>>> p
	{'pgpassfile': '/Users/jwp/.pgpass', 'host': 'localhost', 'user': 'jwp', 'port': 5432}
	>>> pg_param.resolve_password(p)
	>>> p
	{'host': 'localhost', 'password': 'secret', 'user': 'jwp', 'port': 5432}


Defaults
========

The following is a list of default parameters provided by ``collect`` and the
sources of their values:

 ==================== ===================================================================
 Key                  Value
 ==================== ===================================================================
 ``'user'``           `getpass.getuser()` or ``'postgres'``
 ``'host'``           `postgresql.clientparameters.default_host` (``'localhost'``)
 ``'port'``           `postgresql.clientparameters.default_port` (``5432``)
 ``'pgpassfile'``     ``"$HOME/.pgpassfile"`` or ``[PGDATA]`` + ``'pgpass.conf'`` (Win32)
 ``'sslcrtfile'``     ``[PGDATA]`` + ``'postgresql.crt'``
 ``'sslkeyfile'``     ``[PGDATA]`` + ``'postgresql.key'``
 ``'sslrootcrtfile'`` ``[PGDATA]`` + ``'root.crt'``
 ``'sslrootcrlfile'`` ``[PGDATA]`` + ``'root.crl'``
 ==================== ===================================================================

``[PGDATA]`` referenced in the above table is a directory whose path is platform
dependent. On most systems, it is ``"$HOME/.postgresql"``, but on Windows based
systems it is ``"%APPDATA%\postgresql"``

.. note::
 [PGDATA] is *not* an environment variable.


.. _pg_envvars:

PostgreSQL Environment Variables
================================

The following is a list of environment variables that will be collected by the
`postgresql.clientparameter.collect` function using "PG" as the
``environ_prefix`` and the keyword that it will be mapped to:

 ===================== ======================================
 Environment Variable  Keyword
 ===================== ======================================
 ``PGUSER``            ``'user'``
 ``PGDATABASE``        ``'database'``
 ``PGHOST``            ``'host'``
 ``PGPORT``            ``'port'``
 ``PGPASSWORD``        ``'password'``
 ``PGSSLMODE``         ``'sslmode'``
 ``PGSSLKEY``          ``'sslkey'``
 ``PGCONNECT_TIMEOUT`` ``'connect_timeout'``
 ``PGREALM``           ``'kerberos4_realm'``
 ``PGKRBSRVNAME``      ``'kerberos5_service'``
 ``PGPASSFILE``        ``'pgpassfile'``
 ``PGTZ``              ``'settings' = {'timezone': }``
 ``PGDATESTYLE``       ``'settings' = {'datestyle': }``
 ``PGCLIENTENCODING``  ``'settings' = {'client_encoding': }``
 ``PGGEQO``            ``'settings' = {'geqo': }``
 ===================== ======================================


.. _pg_passfile:

PostgreSQL Password File
========================

The password file is a simple newline separated list of ``:`` separated fields. It
is located at ``$HOME/.pgpass`` for most systems and at
``%APPDATA%\postgresql\pgpass.conf`` for Windows based systems. However, the
``PGPASSFILE`` environment variable may be used to override that location.

The lines in the file must be in the following form::

	hostname:port:database:username:password

A single asterisk, ``*``, may be used to indicate that any value will match the
field. However, this only effects fields other than ``password``.

See http://www.postgresql.org/docs/current/static/libpq-pgpass.html for more
details.

Client parameters produced by ``collect`` that have not been processed
by ``resolve_password`` will include a ``'pgpassfile'`` key. This is the value
that ``resolve_password`` will use to locate the pgpassfile to interrogate if a
password key is not present and it is not instructed to prompt for a password.

.. warning::
 Connection creation interfaces will *not* resolve ``'pgpassfile'``, so it is
 important that the parameters produced by ``collect()`` are properly processed
 before an attempt is made to establish a connection.
