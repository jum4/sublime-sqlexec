.. _cluster_management:

******************
Cluster Management
******************

py-postgresql provides cluster management tools in order to give the user
fine-grained control over a PostgreSQL cluster and access to information about an
installation of PostgreSQL.


.. _installation:

Installations
=============

`postgresql.installation.Installation` objects are primarily used to
access PostgreSQL installation information. Normally, they are created using a
dictionary constructed from the output of the pg_config_ executable::

	from postgresql.installation import Installation, pg_config_dictionary
	pg_install = Installation(pg_config_dictionary('/usr/local/pgsql/bin/pg_config'))

The extraction of pg_config_ information is isolated from Installation
instantiation in order to allow Installations to be created from arbitrary
dictionaries. This can be useful in cases where the installation layout is
inconsistent with the standard PostgreSQL installation layout, or if a faux
Installation needs to be created for testing purposes.


Installation Interface Points
-----------------------------

 ``Installation(info)``
  Instantiate an Installation using the given information. Normally, this
  information is extracted from a pg_config_ executable using
  `postgresql.installation.pg_config_dictionary`::

   info = pg_config_dictionary('/usr/local/pgsql/bin/pg_config')
   pg_install = Installation(info)

 ``Installation.version``
  The installation's version string::

   pg_install.version
   'PostgreSQL 9.0devel'

 ``Installation.version_info``
  A tuple containing the version's ``(major, minor, patch, state, level)``.
  Where ``major``, ``minor``, ``patch``, and ``level`` are `int` objects, and
  ``state`` is a `str` object::

   pg_install.version_info
   (9, 0, 0, 'devel', 0)

 ``Installation.ssl``
  A `bool` indicating whether or not the installation has SSL support.

 ``Installation.configure_options``
  The options given to the ``configure`` script that built the installation. The
  options are represented using a dictionary object whose keys are normalized
  long option names, and whose values are the option's argument. If the option
  takes no argument, `True` will be used as the value.

  The normalization of the long option names consists of removing the preceding
  dashes, lowering the string, and replacing any dashes with underscores. For
  instance, ``--enable-debug`` will be ``enable_debug``::

   pg_install.configure_options
   {'enable_debug': True, 'with_libxml': True,
    'enable_cassert': True, 'with_libedit_preferred': True,
    'prefix': '/src/build/pg90', 'with_openssl': True,
    'enable_integer_datetimes': True, 'enable_depend': True}

 ``Installation.paths``
  The paths of the installation as a dictionary where the keys are the path
  identifiers and the values are the absolute file system paths. For instance,
  ``'bindir'`` is associated with ``$PREFIX/bin``, ``'libdir'`` is associated
  with ``$PREFIX/lib``, etc. The paths included in this dictionary are
  listed on the class' attributes: `Installation.pg_directories` and
  `Installation.pg_executables`.

  The keys that point to installation directories are: ``bindir``, ``docdir``,
  ``includedir``, ``pkgincludedir``, ``includedir_server``, ``libdir``,
  ``pkglibdir``, ``localedir``, ``mandir``, ``sharedir``, and ``sysconfdir``.

  The keys that point to installation executables are: ``pg_config``, ``psql``,
  ``initdb``, ``pg_resetxlog``, ``pg_controldata``, ``clusterdb``, ``pg_ctl``,
  ``pg_dump``, ``pg_dumpall``, ``postgres``, ``postmaster``, ``reindexdb``,
  ``vacuumdb``, ``ipcclean``, ``createdb``, ``ecpg``, ``createuser``,
  ``createlang``, ``droplang``, ``dropuser``, and ``pg_restore``.

  .. note:: If the executable does not exist, the value will be `None` instead
            of an absoluate path.

  To get the path to the psql_ executable::

   from postgresql.installation import Installation
   pg_install = Installation('/usr/local/pgsql/bin/pg_config')
   psql_path = pg_install.paths['psql']


Clusters
========

`postgresql.cluster.Cluster` is the class used to manage a PostgreSQL
cluster--a data directory created by initdb_. A Cluster represents a data
directory with respect to a given installation of PostgreSQL, so
creating a `postgresql.cluster.Cluster` object requires a
`postgresql.installation.Installation`, and a
file system path to the data directory.

In part, a `postgresql.cluster.Cluster` is the Python programmer's variant of
the pg_ctl_ command. However, it goes beyond the basic process control
functionality and extends into initialization and configuration as well.

A Cluster manages the server process using the `subprocess` module and
signals. The `subprocess.Popen` object, ``Cluster.daemon_process``, is
retained when the Cluster starts the server process itself. This gives
the Cluster access to the result code of server process when it exits, and the
ability to redirect stderr and stdout to a parameterized file object using
subprocess features.

Despite its use of `subprocess`, Clusters can control a server process
that was *not* started by the Cluster's ``start`` method.


Initializing Clusters
---------------------

`postgresql.cluster.Cluster` provides a method for initializing a
`Cluster`'s data directory, ``init``. This method provides a Python interface to
the PostgreSQL initdb_ command.

``init`` is a regular method and accepts a few keyword parameters. Normally,
parameters are directly mapped to initdb_ command options. However, ``password``
makes use of initdb's capability to read the superuser's password from a file.
To do this, a temporary file is allocated internally by the method::

 from postgresql.installation import Installation, pg_config_dictionary
 from postgresql.cluster import Cluster
 pg_install = Installation(pg_config_dictionary('/usr/local/pgsql/bin/pg_config'))
 pg_cluster = Cluster(pg_install, 'pg_data')
 pg_cluster.init(user = 'pg', password = 'secret', encoding = 'utf-8')

The init method will block until the initdb command is complete. Once
initialized, the Cluster may be configured.


Configuring Clusters
--------------------

A Cluster's `configuration file`_ can be manipulated using the
`Cluster.settings` mapping. The mapping's methods will always access the
configuration file, so it may be desirable to cache repeat reads. Also, if
multiple settings are being applied, using the ``update()`` method may be
important to avoid writing the entire file multiple times::

 pg_cluster.settings.update({'listen_addresses' : 'localhost', 'port' : '6543'})

Similarly, to avoid opening and reading the entire file multiple times,
`Cluster.settings.getset` should be used to retrieve multiple settings::

 d = pg_cluster.settings.getset(set(('listen_addresses', 'port')))
 d
 {'listen_addresses' : 'localhost', 'port' : '6543'}

Values contained in ``settings`` are always Python strings::

 assert pg_cluster.settings['max_connections'].__class__ is str

The ``postgresql.conf`` file is only one part of the server configuration.
Structured access and manipulation of the pg_hba_ file is not
supported. Clusters only provide the file path to the pg_hba_ file::

 hba = open(pg_cluster.hba_file)

If the configuration of the Cluster is altered while the server process is
running, it may be necessary to signal the process that configuration changes
have been made. This signal can be sent using the ``Cluster.reload()`` method.
``Cluster.reload()`` will send a SIGHUP signal to the server process. However,
not all changes to configuration settings can go into effect after calling
``Cluster.reload()``. In those cases, the server process will need to be
shutdown and started again.


Controlling Clusters
--------------------

The server process of a Cluster object can be controlled with the ``start()``,
``stop()``, ``shutdown()``, ``kill()``, and ``restart()`` methods.
These methods start the server process, signal the server process, or, in the
case of restart, a combination of the two.

When a Cluster starts the server process, it's ran as a subprocess. Therefore,
if the current process exits, the server process will exit as well. ``start()``
does *not* automatically daemonize the server process.

.. note:: Under Microsoft Windows, above does not hold true. The server process
          will continue running despite the exit of the parent process.

To terminate a server process, one of these three methods should be called:
``stop``, ``shutdown``, or ``kill``. ``stop`` is a graceful shutdown and will
*wait for all clients to disconnect* before shutting down. ``shutdown`` will
close any open connections and safely shutdown the server process.
``kill`` will immediately terminate the server process leading to recovery upon
starting the server process again.

.. note:: Using ``kill`` may cause shared memory to be leaked.

Normally, `Cluster.shutdown` is the appropriate way to terminate a server
process.


Cluster Interface Points
------------------------

Methods and properties available on `postgresql.cluster.Cluster` instances:

 ``Cluster(installation, data_directory)``
  Create a `postgresql.cluster.Cluster` object for the specified
  `postgresql.installation.Installation`, and ``data_directory``.

  The ``data_directory`` must be an absoluate file system path. The directory
  does *not* need to exist. The ``init()`` method may later be used to create
  the cluster.

 ``Cluster.installation``
  The Cluster's `postgresql.installation.Installation` instance.

 ``Cluster.data_directory``
  The absolute path to the PostgreSQL data directory.
  This directory may not exist.

 ``Cluster.init([encoding = None[, user = None[, password = None]]])``
  Run the `initdb`_ executable of the configured installation to initialize the
  cluster at the configured data directory, `Cluster.data_directory`.

  ``encoding`` is mapped to ``-E``, the default database encoding. By default,
  the encoding is determined from the environment's locale.

  ``user`` is mapped to ``-U``, the database superuser name. By default, the
  current user's name.

  ``password`` is ultimately mapped to ``--pwfile``. The argument given to the
  long option is actually a path to the temporary file that holds the given
  password.

  Raises `postgresql.cluster.InitDBError` when initdb_ returns a non-zero result
  code.

  Raises `postgresql.cluster.ClusterInitializationError` when there is no
  initdb_ in the Installation.

 ``Cluster.initialized()``
  Whether or not the data directory exists, *and* if it looks like a PostgreSQL
  data directory. Meaning, the directory must contain a ``postgresql.conf`` file
  and a ``base`` directory.

 ``Cluster.drop()``
  Shutdown the Cluster's server process and completely remove the
  `Cluster.data_directory` from the file system.

 ``Cluster.pid()``
  The server's process identifier as a Python `int`. `None` if there is no
  server process running.
  This is a method rather than a property as it may read the PID from a file
  in cases where the server process was not started by the Cluster.

 ``Cluster.start([logfile = None[, settings = None]])``
  Start the PostgreSQL server process for the Cluster if it is not
  already running. This will execute postgres_ as a subprocess.

  If ``logfile``, an opened and writable file object, is given, stderr and
  stdout will be redirected to that file. By default, both stderr and stdout are
  closed.

  If ``settings`` is given, the mapping or sequence of pairs will be used as
  long options to the subprocess. For each item, ``--{key}={value}`` will be
  given as an argument to the subprocess.

 ``Cluster.running()``
  Whether or not the cluster's server process is running. Returns `True` or
  `False`. Even if `True` is returned, it does *not* mean that the server
  process is ready to accept connections.

 ``Cluster.ready_for_connections()``
  Whether or not the Cluster is ready to accept connections. Usually called
  after `Cluster.start`.

  Returns `True` when the Cluster can accept connections, `False` when it
  cannot, and `None` if the Cluster's server process is not running at all.

 ``Cluster.wait_until_started([timeout = 10[, delay = 0.05]])``
  Blocks the process until the cluster is identified as being ready for
  connections. Usually called after ``Cluster.start()``.

  Raises `postgresql.cluster.ClusterNotRunningError` if the server process is
  not running at all.

  Raises `postgresql.cluster.ClusterTimeoutError` if
  `Cluster.ready_for_connections()` does not return `True` within the given
  `timeout` period.

  Raises `postgresql.cluster.ClusterStartupError` if the server process
  terminates while polling for readiness.

  ``timeout`` and ``delay`` are both in seconds. Where ``timeout`` is the
  maximum time to wait for the Cluster to be ready for connections, and
  ``delay`` is the time to sleep between calls to
  `Cluster.ready_for_connections()`.

 ``Cluster.stop()``
  Signal the cluster to shutdown when possible. The *server* will wait for all
  clients to disconnect before shutting down.

 ``Cluster.shutdown()``
  Signal the cluster to shutdown immediately. Any open client connections will
  be closed.

 ``Cluster.kill()``
  Signal the absolute destruction of the server process(SIGKILL).
  *This will require recovery when the cluster is started again.*
  *Shared memory may be leaked.*

 ``Cluster.wait_until_stopped([timeout = 10[, delay = 0.05]])``
  Blocks the process until the cluster is identified as being shutdown. Usually
  called after `Cluster.stop` or `Cluster.shutdown`.

  Raises `postgresql.cluster.ClusterTimeoutError` if
  `Cluster.ready_for_connections` does not return `None` within the given
  `timeout` period.

 ``Cluster.reload()``
  Signal the server that it should reload its configuration files(SIGHUP).
  Usually called after manipulating `Cluster.settings` or modifying the
  contents of `Cluster.hba_file`.

 ``Cluster.restart([logfile = None[, settings = None[, timeout = 10]]])``
  Stop the server process, wait until it is stopped, start the server
  process, and wait until it has started.

  .. note:: This calls ``Cluster.stop()``, so it will wait until clients
            disconnect before starting up again.

  The ``logfile`` and ``settings`` parameters will be given to `Cluster.start`.
  ``timeout`` will be given to `Cluster.wait_until_stopped` and
  `Cluster.wait_until_started`.

 ``Cluster.settings``
  A `collections.Mapping` interface to the ``postgresql.conf`` file of the
  cluster.

  A notable extension to the mapping interface is the ``getset`` method. This
  method will return a dictionary object containing the settings whose names
  were contained in the `set` object given to the method.
  This method should be used when multiple settings need to be retrieved from
  the configuration file.

 ``Cluster.hba_file``
  The path to the cluster's pg_hba_ file. This property respects the HBA file
  location setting in ``postgresql.conf``. Usually, ``$PGDATA/pg_hba.conf``.

 ``Cluster.daemon_path``
  The path to the executable to use to start the server process.

 ``Cluster.daemon_process``
  The `subprocess.Popen` instance of the server process. `None` if the server
  process was not started or was not started using the Cluster object.


.. _pg_hba: http://www.postgresql.org/docs/current/static/auth-pg-hba-conf.html
.. _pg_config: http://www.postgresql.org/docs/current/static/app-pgconfig.html
.. _initdb: http://www.postgresql.org/docs/current/static/app-initdb.html
.. _psql: http://www.postgresql.org/docs/current/static/app-psql.html
.. _postgres: http://www.postgresql.org/docs/current/static/app-postgres.html
.. _pg_ctl: http://www.postgresql.org/docs/current/static/app-pg-ctl.html
.. _configuration file: http://www.postgresql.org/docs/current/static/runtime-config.html
