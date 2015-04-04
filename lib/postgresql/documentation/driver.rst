.. _db_interface:

******
Driver
******

`postgresql.driver` provides a PG-API, `postgresql.api`, interface to a
PostgreSQL server using PQ version 3.0 to facilitate communication. It makes
use of the protocol's extended features to provide binary datatype transmission
and protocol level prepared statements for strongly typed parameters.

`postgresql.driver` currently supports PostgreSQL servers as far back as 8.0.
Prior versions are not tested. While any version of PostgreSQL supporting
version 3.0 of the PQ protocol *should* work, many features may not work due to
absent functionality in the remote end.

For DB-API 2.0 users, the driver module is located at
`postgresql.driver.dbapi20`. The DB-API 2.0 interface extends PG-API. All of the
features discussed in this chapter are available on DB-API connections.

.. warning::
   PostgreSQL versions 8.1 and earlier do not support standard conforming
   strings. In order to avoid subjective escape methods on connections,
   `postgresql.driver.pq3` enables the ``standard_conforming_strings`` setting
   by default. Greater care must be taken when working versions that do not
   support standard strings.
   **The majority of issues surrounding the interpolation of properly quoted literals can be easily avoided by using parameterized statements**.

The following identifiers are regularly used as shorthands for significant
interface elements:

 ``db``
  `postgresql.api.Connection`, a database connection. `Connections`_

 ``ps``
  `postgresql.api.Statement`, a prepared statement. `Prepared Statements`_

 ``c``
  `postgresql.api.Cursor`, a cursor; the results of a prepared statement.
  `Cursors`_

 ``C``
  `postgresql.api.Connector`, a connector. `Connectors`_


Establishing a Connection
=========================

There are many ways to establish a `postgresql.api.Connection` to a
PostgreSQL server using `postgresql.driver`. This section discusses those,
connection creation, interfaces.


`postgresql.open`
-----------------

In the root package module, the ``open()`` function is provided for accessing
databases using a locator string and optional connection keywords. The string
taken by `postgresql.open` is a URL whose components make up the client
parameters::

	>>> db = postgresql.open("pq://localhost/postgres")

This will connect to the host, ``localhost`` and to the database named
``postgres`` via the ``pq`` protocol. open will inherit client parameters from
the environment, so the user name given to the server will come from
``$PGUSER``, or if that is unset, the result of `getpass.getuser`--the username
of the user running the process. The user's "pgpassfile" will even be
referenced if no password is given::

	>>> db = postgresql.open("pq://username:password@localhost/postgres")

In this case, the password *is* given, so ``~/.pgpass`` would never be
referenced. The ``user`` client parameter is also given, ``username``, so
``$PGUSER`` or `getpass.getuser` will not be given to the server.

Settings can also be provided by the query portion of the URL::

	>>> db = postgresql.open("pq://user@localhost/postgres?search_path=public&timezone=mst")

The above syntax ultimately passes the query as settings(see the description of
the ``settings`` keyword in `Connection Keywords`). Driver parameters require a
distinction. This distinction is made when the setting's name is wrapped in
square-brackets, '[' and ']':

	>>> db = postgresql.open("pq://user@localhost/postgres?[sslmode]=require&[connect_timeout]=5")

``sslmode`` and ``connect_timeout`` are driver parameters. These are never sent
to the server, but if they were not in square-brackets, they would be, and the
driver would never identify them as driver parameters.

The general structure of a PQ-locator is::

	protocol://user:password@host:port/database?[driver_setting]=value&server_setting=value

Optionally, connection keyword arguments can be used to override anything given
in the locator::

	>>> db = postgresql.open("pq://user:secret@host", password = "thE_real_sekrat")

Or, if the locator is not desired, individual keywords can be used exclusively::

	>>> db = postgresql.open(user = 'user', host = 'localhost', port = 6543)

In fact, all arguments to `postgresql.open` are optional as all arguments are
keywords; ``iri`` is merely the first keyword argument taken by
`postgresql.open`. If the environment has all the necessary parameters for a
successful connection, there is no need to pass anything to open::

	>>> db = postgresql.open()

For a complete list of keywords that `postgresql.open` can accept, see
`Connection Keywords`_.
For more information about the environment variables, see :ref:`pg_envvars`.
For more information about the ``pgpassfile``, see :ref:`pg_passfile`.

`postgresql.driver.connect`
---------------------------

`postgresql.open` is a high-level interface to connection creation. It provides
password resolution services and client parameter inheritance. For some
applications, this is undesirable as such implicit inheritance may lead to
failures due to unanticipated parameters being used. For those applications,
use of `postgresql.open` is not recommended. Rather, `postgresql.driver.connect`
should be used when explicit parameterization is desired by an application:

	>>> import postgresql.driver as pg_driver
	>>> db = pg_driver.connect(
	...  user = 'usename',
	...  password = 'secret',
	...  host = 'localhost',
	...  port = 5432
	... )

This will create a connection to the server listening on port ``5432``
on the host ``localhost`` as the user ``usename`` with the password ``secret``.

.. note::
 `connect` will *not* inherit parameters from the environment as libpq-based drivers do.

See `Connection Keywords`_ for a full list of acceptable keyword parameters and
their meaning.


Connectors
----------

Connectors are the supporting objects used to instantiate a connection. They
exist for the purpose of providing connections with the necessary abstractions
for facilitating the client's communication with the server, *and to act as a
container for the client parameters*. The latter purpose is of primary interest
to this section.

Each connection object is associated with its connector by the ``connector``
attribute on the connection. This provides the user with access to the
parameters used to establish the connection in the first place, and the means to
create another connection to the same server. The attributes on the connector
should *not* be altered. If parameter changes are needed, a new connector should
be created.

The attributes available on a connector are consistent with the names of the
connection parameters described in `Connection Keywords`_, so that list can be
used as a reference to identify the information available on the connector.

Connectors fit into the category of "connection creation interfaces", so
connector instantiation normally takes the same parameters that the
`postgresql.driver.connect` function takes.

.. note::
 Connector implementations are specific to the transport, so keyword arguments
 like ``host`` and ``port`` aren't supported by the ``Unix`` connector.

The driver, `postgresql.driver.default` provides a set of connectors for making
a connection:

 ``postgresql.driver.default.host(...)``
  Provides a ``getaddrinfo()`` abstraction for establishing a connection.

 ``postgresql.driver.default.ip4(...)``
  Connect to a single IPv4 addressed host.

 ``postgresql.driver.default.ip6(...)``
  Connect to a single IPv6 addressed host.

 ``postgresql.driver.default.unix(...)``
  Connect to a single unix domain socket. Requires the ``unix`` keyword which
  must be an absolute path to the unix domain socket to connect to.

``host`` is the usual connector used to establish a connection::

	>>> C = postgresql.driver.default.host(
	...  user = 'auser',
	...  host = 'foo.com',
	...  port = 5432)
	>>> # create
	>>> db = C()
	>>> # establish
	>>> db.connect()

If a constant internet address is used, ``ip4`` or ``ip6`` can be used::

	>>> C = postgresql.driver.default.ip4(user='auser', host='127.0.0.1', port=5432)
	>>> db = C()
	>>> db.connect()

Additionally, ``db.connect()`` on ``db.__enter__()`` for with-statement support:

	>>> with C() as db:
	...  ...

Connectors are constant. They have no knowledge of PostgreSQL service files,
environment variables or LDAP services, so changes made to those facilities
will *not* be reflected in a connector's configuration. If the latest
information from any of these sources is needed, a new connector needs to be
created as the credentials have changed.

.. note::
 ``host`` connectors use ``getaddrinfo()``, so if DNS changes are made, 
 new connections *will* use the latest information.


Connection Keywords
-------------------

The following is a list of keywords accepted by connection creation
interfaces:

 ``user``
  The user to connect as.

 ``password``
  The user's password.

 ``database``
  The name of the database to connect to. (PostgreSQL defaults it to `user`)

 ``host``
  The hostname or IP address to connect to.

 ``port``
  The port on the host to connect to.

 ``unix``
  The unix domain socket to connect to. Exclusive with ``host`` and ``port``.
  Expects a string containing the *absolute path* to the unix domain socket to
  connect to.

 ``settings``
  A dictionary or key-value pair sequence stating the parameters to give to the
  database. These settings are included in the startup packet, and should be
  used carefully as when an invalid setting is given, it will cause the
  connection to fail.

 ``connect_timeout``
  Amount of time to wait for a connection to be made. (in seconds)

 ``server_encoding``
  Hint given to the driver to properly encode password data and some information
  in the startup packet.
  This should only be used in cases where connections cannot be made due to
  authentication failures that occur while using known-correct credentials.

 ``sslmode``
  ``'disable'``
   Don't allow SSL connections.
  ``'allow'``
   Try without SSL first, but if that doesn't work, try with.
  ``'prefer'``
   Try SSL first, then without.
  ``'require'``
   Require an SSL connection.

 ``sslcrtfile``
  Certificate file path given to `ssl.wrap_socket`.

 ``sslkeyfile``
  Key file path given to `ssl.wrap_socket`.

 ``sslrootcrtfile``
  Root certificate file path given to `ssl.wrap_socket`

 ``sslrootcrlfile``
  Revocation list file path. [Currently not checked.]


Connections
===========

`postgresql.open` and `postgresql.driver.connect` provide the means to
establish a connection. Connections provide a `postgresql.api.Database`
interface to a PostgreSQL server; specifically, a `postgresql.api.Connection`.

Connections are one-time objects. Once, it is closed or lost, it can longer be
used to interact with the database provided by the server. If further use of the
server is desired, a new connection *must* be established.

.. note::
 Cannot connect failures, exceptions raised on ``connect()``, are also terminal.

In cases where operations are performed on a closed connection, a
`postgresql.exceptions.ConnectionDoesNotExistError` will be raised.


Database Interface Points
-------------------------

After a connection is established::

	>>> import postgresql
	>>> db = postgresql.open(...)

The methods and properties on the connection object are ready for use:

 ``Connection.prepare(sql_statement_string)``
  Create a `postgresql.api.Statement` object for querying the database.
  This provides an "SQL statement template" that can be executed multiple times.
  See `Prepared Statements`_ for more information.

 ``Connection.proc(procedure_id)``
  Create a `postgresql.api.StoredProcedure` object referring to a stored
  procedure on the database. The returned object will provide a
  `collections.Callable` interface to the stored procedure on the server. See
  `Stored Procedures`_ for more information.

 ``Connection.statement_from_id(statement_id)``
  Create a `postgresql.api.Statement` object from an existing statement
  identifier. This is used in cases where the statement was prepared on the
  server. See `Prepared Statements`_ for more information.

 ``Connection.cursor_from_id(cursor_id)``
  Create a `postgresql.api.Cursor` object from an existing cursor identifier.
  This is used in cases where the cursor was declared on the server. See
  `Cursors`_ for more information.

 ``Connection.do(language, source)``
  Execute a DO statement on the server using the specified language.
  *DO statements are available on PostgreSQL 9.0 and greater.*
  *Executing this method on servers that do not support DO statements will*
  *likely cause a SyntaxError*.

 ``Connection.execute(sql_statements_string)``
  Run a block of SQL on the server. This method returns `None` unless an error
  occurs. If errors occur, the processing of the statements will stop and the
  error will be raised.

 ``Connection.xact(isolation = None, mode = None)``
  The `postgresql.api.Transaction` constructor for creating transactions.
  This method creates a transaction reference. The transaction will not be
  started until it's instructed to do so. See `Transactions`_ for more
  information.

 ``Connection.settings``
  A property providing a `collections.MutableMapping` interface to the
  database's SQL settings. See `Settings`_ for more information.

 ``Connection.clone()``
  Create a new connection object based on the same factors that were used to
  create ``db``. The new connection returned will already be connected.

 ``Connection.msghook(msg)``
  By default, the `msghook` attribute does not exist. If set to a callable, any
  message that occurs during an operation of the database or an operation of a
  database derived object will be given to the callable. See the
  `Database Messages`_ section for more information.

 ``Connection.listen(*channels)``
  Start listening for asynchronous notifications in the specified channels.
  Sends a batch of ``LISTEN`` statements to the server.

 ``Connection.unlisten(*channels)``
  Stop listening for asynchronous notifications in the specified channels.
  Sends a batch of ``UNLISTEN`` statements to the server.

 ``Connection.listening_channels()``
  Return an iterator producing the channel names that are currently being
  listened to.

 ``Connection.notify(*channels, **channel_and_payload)``
  NOTIFY the channels with the given payload. Sends a batch of ``NOTIFY``
  statements to the server.

  Equivalent to issuing "NOTIFY <channel>" or "NOTIFY <channel>, <payload>"
  for each item in `channels` and `channel_and_payload`. All NOTIFYs issued
  will occur in the same transaction, regardless of auto-commit.

  The items in `channels` can either be a string or a tuple. If a string,
  no payload is given, but if an item is a `builtins.tuple`, the second item
  in the pair will be given as the payload, and the first as the channel.
  `channels` offers a means to issue NOTIFYs in guaranteed order::

   >>> db.notify('channel1', ('different_channel', 'payload'))

  In the above, ``NOTIFY "channel1";`` will be issued first, followed by
  ``NOTIFY "different_channel", 'payload';``.

  The items in `channel_and_payload` are all payloaded NOTIFYs where the
  keys are the channels and the values are the payloads. Order is undefined::

   >>> db.notify(channel_name = 'payload_data')

  `channels` and `channels_and_payload` can be used together. In such cases all
  NOTIFY statements generated from `channels_and_payload` will follow those in
  `channels`.

 ``Connection.iternotifies(timeout = None)``
  Return an iterator to the NOTIFYs received on the connection. The iterator
  will yield notification triples consisting of ``(channel, payload, pid)``.
  While iterating, the connection should *not* be used in other threads.
  The optional timeout can be used to enable "idle" events in which `None`
  objects will be yielded by the iterator.
  See :ref:`notifyman` for details.

When a connection is established, certain pieces of information are collected from
the backend. The following are the attributes set on the connection object after
the connection is made:

 ``Connection.version``
  The version string of the *server*; the result of ``SELECT version()``.

 ``Connection.version_info``
  A ``sys.version_info`` form of the ``server_version`` setting. eg.
  ``(8, 1, 2, 'final', 0)``.

 ``Connection.security``
  `None` if no security. ``'ssl'`` if SSL is enabled.

 ``Connection.backend_id``
  The process-id of the backend process.

 ``Connection.backend_start``
  When backend was started. ``datetime.datetime`` instance.

 ``Connection.client_address``
  The address of the client that the backend is communicating with.

 ``Connection.client_port``
  The port of the client that the backend is communicating with.

 ``Connection.fileno()``
  Method to get the file descriptor number of the connection's socket. This
  method will return `None` if the socket object does not have a ``fileno``.
  Under normal circumstances, it will return an `int`.

The ``backend_start``, ``client_address``, and ``client_port`` are collected
from pg_stat_activity. If this information is unavailable, the attributes will
be `None`.


Prepared Statements
===================

Prepared statements are the primary entry point for initiating an operation on
the database. Prepared statement objects represent a request that will, likely,
be sent to the database at some point in the future. A statement is a single
SQL command.

The ``prepare`` entry point on the connection provides the standard method for
creating a `postgersql.api.Statement` instance bound to the
connection(``db``) from an SQL statement string::

	>>> ps = db.prepare("SELECT 1")
	>>> ps()
	[(1,)]

Statement objects may also be created from a statement identifier using the
``statement_from_id`` method on the connection. When this method is used, the
statement must have already been prepared or an error will be raised.

	>>> db.execute("PREPARE a_statement_id AS SELECT 1;")
	>>> ps = db.statement_from_id('a_statement_id')
	>>> ps()
	[(1,)]

When a statement is executed, it binds any given parameters to a *new* cursor
and the entire result-set is returned.

Statements created using ``prepare()`` will leverage garbage collection in order
to automatically close statements that are no longer referenced. However,
statements created from pre-existing identifiers, ``statement_from_id``, must
be explicitly closed if the statement is to be discarded.

Statement objects are one-time objects. Once closed, they can no longer be used.


Statement Interface Points
--------------------------

Prepared statements can be executed just like functions:

	>>> ps = db.prepare("SELECT 'hello, world!'")
	>>> ps()
	[('hello, world!',)]

The default execution method, ``__call__``, produces the entire result set. It
is the simplest form of statement execution. Statement objects can be executed in
different ways to accommodate for the larger results or random access(scrollable
cursors).

Prepared statement objects have a few execution methods:

 ``Statement(*parameters)``
  As shown before, statement objects can be invoked like a function to get
  the statement's results.

 ``Statement.rows(*parameters)``
  Return a iterator to all the rows produced by the statement. This
  method will stream rows on demand, so it is ideal for situations where
  each individual row in a large result-set must be processed.

 ``iter(Statement)``
  Convenience interface that executes the ``rows()`` method without arguments.
  This enables the following syntax:

  >>> for table_name, in db.prepare("SELECT table_name FROM information_schema.tables"):
  ...  print(table_name)

 ``Statement.column(*parameters)``
  Return a iterator to the first column produced by the statement. This
  method will stream values on demand, and *should* only be used with statements
  that have a single column; otherwise, bandwidth will ultimately be wasted as
  the other columns will be dropped.
  *This execution method cannot be used with COPY statements.*

 ``Statement.first(*parameters)``
  For simple statements, cursor objects are unnecessary.
  Consider the data contained in ``c`` from above, 'hello world!'. To get at this
  data directly from the ``__call__(...)`` method, it looks something like::

	>>> ps = db.prepare("SELECT 'hello, world!'")
	>>> ps()[0][0]
	'hello, world!'

  To simplify access to simple data, the ``first`` method will simply return
  the "first" of the result set::

   >>> ps.first()
   'hello, world!'

  The first value.
   When the result set consists of a single column, ``first()`` will return
   that column in the first row.

  The first row.
   When the result set consists of multiple columns, ``first()`` will return
   that first row.

  The first, and only, row count.
   When DML--for instance, an INSERT-statement--is executed, ``first()`` will
   return the row count returned by the statement as an integer.

   .. note::
    DML that returns row data, RETURNING, will *not* return a row count.

  The result set created by the statement determines what is actually returned.
  Naturally, a statement used with ``first()`` should be crafted with these
  rules in mind.

 ``Statement.chunks(*parameters)``
  This access point is designed for situations where rows are being streamed out
  quickly. It is a method that returns a ``collections.Iterator`` that produces
  *sequences* of rows. This is the most efficient way to get rows from the
  database. The rows in the sequences are ``builtins.tuple`` objects.

 ``Statement.declare(*parameters)``
  Create a scrollable cursor with hold. This returns a `postgresql.api.Cursor`
  ready for accessing random rows in the result-set. Applications that use the
  database to support paging can use this method to manage the view.

 ``Statement.close()``
  Close the statement inhibiting further use.

 ``Statement.load_rows(collections.Iterable(parameters))``
  Given an iterable producing parameters, execute the statement for each
  iteration. Always returns `None`.

 ``Statement.load_chunks(collections.Iterable(collections.Iterable(parameters)))``
  Given an iterable of iterables producing parameters, execute the statement
  for each parameter produced. However, send the all execution commands with
  the corresponding parameters of each chunk before reading any results.
  Always returns `None`. This access point is designed to be used in conjunction
  with ``Statement.chunks()`` for transferring rows from one connection to another with
  great efficiency::

   >>> dst.prepare(...).load_chunks(src.prepare(...).chunks())

 ``Statement.clone()``
  Create a new statement object based on the same factors that were used to
  create ``ps``.

 ``Statement.msghook(msg)``
  By default, the `msghook` attribute does not exist. If set to a callable, any
  message that occurs during an operation of the statement or an operation of a
  statement derived object will be given to the callable. See the
  `Database Messages`_ section for more information.

In order to provide the appropriate type transformations, the driver must
acquire metadata about the statement's parameters and results. This data is
published via the following properties on the statement object:

 ``Statement.sql_parameter_types``
  A sequence of SQL type names specifying the types of the parameters used in
  the statement.

 ``Statement.sql_column_types``
  A sequence of SQL type names specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``Statement.pg_parameter_types``
  A sequence of PostgreSQL type Oid's specifying the types of the parameters
  used in the statement.

 ``Statement.pg_column_types``
  A sequence of PostgreSQL type Oid's specifying the types of the columns produced by
  the statement. `None` if the statement does not return row-data.

 ``Statement.parameter_types``
  A sequence of Python types that the statement expects.

 ``Statement.column_types``
  A sequence of Python types that the statement will produce.

 ``Statement.column_names``
  A sequence of `str` objects specifying the names of the columns produced by
  the statement. `None` if the statement does not return row-data.

The indexes of the parameter sequences correspond to the parameter's
identifier, N+1: ``sql_parameter_types[0]`` -> ``'$1'``.

	>>> ps = db.prepare("SELECT $1::integer AS intname, $2::varchar AS chardata")
	>>> ps.sql_parameter_types
	('INTEGER','VARCHAR')
	>>> ps.sql_column_types
	('INTEGER','VARCHAR')
	>>> ps.column_names
	('intname','chardata')
	>>> ps.column_types
	(<class 'int'>, <class 'str'>)


Parameterized Statements
------------------------

Statements can take parameters. Using statement parameters is the recommended
way to interrogate the database when variable information is needed to formulate
a complete request. In order to do this, the statement must be defined using
PostgreSQL's positional parameter notation. ``$1``, ``$2``, ``$3``, etc::

	>>> ps = db.prepare("SELECT $1")
	>>> ps('hello, world!')[0][0]
	'hello, world!'

PostgreSQL determines the type of the parameter based on the context of the
parameter's identifier::

	>>> ps = db.prepare(
	...  "SELECT * FROM information_schema.tables WHERE table_name = $1 LIMIT $2"
	... )
	>>> ps("tables", 1)
	[('postgres', 'information_schema', 'tables', 'VIEW', None, None, None, None, None, 'NO', 'NO', None)]

Parameter ``$1`` in the above statement will take on the type of the
``table_name`` column and ``$2`` will take on the type required by the LIMIT
clause(text and int8).

However, parameters can be forced to a specific type using explicit casts:

	>>> ps = db.prepare("SELECT $1::integer")
	>>> ps.first(-400)
	-400

Parameters are typed. PostgreSQL servers provide the driver with the
type information about a positional parameter, and the serialization routine
will raise an exception if the given object is inappropriate. The Python
types expected by the driver for a given SQL-or-PostgreSQL type are listed
in `Type Support`_.

This usage of types is not always convenient. Notably, the `datetime` module
does not provide a friendly way for a user to express intervals, dates, or
times. There is a likely inclination to forego these parameter type
requirements.

In such cases, explicit casts can be made to work-around the type
requirements::

	>>> ps = db.prepare("SELECT $1::text::date")
	>>> ps.first('yesterday')
	datetime.date(2009, 3, 11)

The parameter, ``$1``, is given to the database as a string, which is then
promptly cast into a date. Of course, without the explicit cast as text, the
outcome would be different::

	>>> ps = db.prepare("SELECT $1::date")
	>>> ps.first('yesterday')
	Traceback:
	 ...
	postgresql.exceptions.ParameterError

The function that processes the parameter expects a `datetime.date` object, and
the given `str` object does not provide the necessary interfaces for the
conversion, so the driver raises a `postgresql.exceptions.ParameterError` from
the original conversion exception.


Inserting and DML
-----------------

Loading data into the database is facilitated by prepared statements. In these
examples, a table definition is necessary for a complete illustration::

	>>> db.execute(
	... 	"""
	... CREATE TABLE employee (
	... 	employee_name text,
	... 	employee_salary numeric,
	... 	employee_dob date,
	... 	employee_hire_date date
	... );
	... 	"""
	... )

Create an INSERT statement using ``prepare``::

	>>> mkemp = db.prepare("INSERT INTO employee VALUES ($1, $2, $3, $4)")

And add "Mr. Johnson" to the table::

	>>> import datetime
	>>> r = mkemp(
	... 	"John Johnson",
	... 	"92000",
	... 	datetime.date(1950, 12, 10),
	... 	datetime.date(1998, 4, 23)
	... )
	>>> print(r[0])
	INSERT
	>>> print(r[1])
	1

The execution of DML will return a tuple. This tuple contains the completed
command name and the associated row count.

Using the call interface is fine for making a single insert, but when multiple
records need to be inserted, it's not the most efficient means to load data. For
multiple records, the ``ps.load_rows([...])`` provides an efficient way to load
large quantities of structured data::

	>>> from datetime import date
	>>> mkemp.load_rows([
	...  ("Jack Johnson", "85000", date(1962, 11, 23), date(1990, 3, 5)),
	...  ("Debra McGuffer", "52000", date(1973, 3, 4), date(2002, 1, 14)),
	...  ("Barbara Smith", "86000", date(1965, 2, 24), date(2005, 7, 19)),
	... ])

While small, the above illustrates the ``ps.load_rows()`` method taking an
iterable of tuples that provides parameters for the each execution of the
statement.

``load_rows`` is also used to support ``COPY ... FROM STDIN`` statements::

	>>> copy_emps_in = db.prepare("COPY employee FROM STDIN")
	>>> copy_emps_in.load_rows([
	...  b'Emp Name1\t72000\t1970-2-01\t1980-10-22\n',
	...  b'Emp Name2\t62000\t1968-9-11\t1985-11-1\n',
	...  b'Emp Name3\t62000\t1968-9-11\t1985-11-1\n',
	... ])

Copy data goes in as bytes and come out as bytes regardless of the type of COPY
taking place. It is the user's obligation to make sure the row-data is in the
appropriate encoding.


COPY Statements
---------------

`postgresql.driver` transparently supports PostgreSQL's COPY command. To the
user, COPY will act exactly like other statements that produce tuples; COPY
tuples, however, are `bytes` objects. The only distinction in usability is that
the COPY *should* be completed before other actions take place on the
connection--this is important when a COPY is invoked via ``rows()`` or
``chunks()``.

In situations where other actions are invoked during a ``COPY TO STDOUT``, the
entire result set of the COPY will be read. However, no error will be raised so
long as there is enough memory available, so it is *very* desirable to avoid
doing other actions on the connection while a COPY is active.

In situations where other actions are invoked during a ``COPY FROM STDIN``, a
COPY failure error will occur. The driver manages the connection state in such
a way that will purposefully cause the error as the COPY was inappropriately
interrupted. This not usually a problem as ``load_rows(...)`` and
``load_chunks(...)`` methods must complete the COPY command before returning.

Copy data is always transferred using ``bytes`` objects. Even in cases where the
COPY is not in ``BINARY`` mode. Any needed encoding transformations *must* be
made the caller. This is done to avoid any unnecessary overhead by default::

	>>> ps = db.prepare("COPY (SELECT i FROM generate_series(0, 99) AS g(i)) TO STDOUT")
	>>> r = ps()
	>>> len(r)
	100
	>>> r[0]
	b'0\n'
	>>> r[-1]
	b'99\n'

Of course, invoking a statement that way will read the entire result-set into
memory, which is not usually desirable for COPY. Using the ``chunks(...)``
iterator is the *fastest* way to move data::

	>>> ci = ps.chunks()
	>>> import sys
	>>> for rowset in ps.chunks():
	...  sys.stdout.buffer.writelines(rowset)
	...
	<lots of data>

``COPY FROM STDIN`` commands are supported via
`postgresql.api.Statement.load_rows`. Each invocation to
``load_rows`` is a single invocation of COPY. ``load_rows`` takes an iterable of
COPY lines to send to the server::

	>>> db.execute("""
	... CREATE TABLE sample_copy (
	...	sc_number int,
	...	sc_text text
	... );
	... """)
	>>> copyin = db.prepare('COPY sample_copy FROM STDIN')
	>>> copyin.load_rows([
	... 	b'123\tone twenty three\n',
	... 	b'350\ttree fitty\n',
	... ])

For direct connection-to-connection COPY, use of ``load_chunks(...)`` is
recommended as it will provide the most efficient transfer method::

	>>> copyout = src.prepare('COPY atable TO STDOUT')
	>>> copyin = dst.prepare('COPY atable FROM STDIN')
	>>> copyin.load_chunks(copyout.chunks())

Specifically, each chunk of row data produced by ``chunks()`` will be written in
full by ``load_chunks()`` before getting another chunk to write.


Cursors
=======

When a prepared statement is declared, ``ps.declare(...)``, a
`postgresql.api.Cursor` is created and returned for random access to the rows in
the result set. Direct use of cursors is primarily useful for applications that
need to implement paging. For situations that need to iterate over the result
set, the ``ps.rows(...)`` or ``ps.chunks(...)`` execution methods should be
used.

Cursors can also be created directly from ``cursor_id``'s using the
``cursor_from_id`` method on connection objects::

	>>> db.execute('DECLARE the_cursor_id CURSOR WITH HOLD FOR SELECT 1;')
	>>> c = db.cursor_from_id('the_cursor_id')
	>>> c.read()
	[(1,)]
	>>> c.close()

.. hint::
 If the cursor that needs to be opened is going to be treated as an iterator,
 then a FETCH-statement should be prepared instead using ``cursor_from_id``.

Like statements created from an identifier, cursors created from an identifier
must be explicitly closed in order to destroy the object on the server.
Likewise, cursors created from statement invocations will be automatically
released when they are no longer referenced.

.. note::
 PG-API cursors are a direct interface to single result-set SQL cursors. This
 is in contrast with DB-API cursors, which have interfaces for dealing with
 multiple result-sets. There is no execute method on PG-API cursors.


Cursor Interface Points
-----------------------

For cursors that return row data, these interfaces are provided for accessing
those results:

 ``Cursor.read(quantity = None, direction = None)``
  This method name is borrowed from `file` objects, and are semantically
  similar. However, this being a cursor, rows are returned instead of bytes or
  characters. When the number of rows returned is less then the quantity
  requested, it means that the cursor has been exhausted in the configured
  direction. The ``direction`` argument can be either ``'FORWARD'`` or `True`
  to FETCH FORWARD, or ``'BACKWARD'`` or `False` to FETCH BACKWARD.

  Like, ``seek()``, the ``direction`` *property* on the cursor object effects
  this method.

 ``Cursor.seek(position[, whence = 0])``
  When the cursor is scrollable, this seek interface can be used to move the
  position of the cursor. See `Scrollable Cursors`_ for more information.

 ``next(Cursor)``
  This fetches the next row in the cursor object. Cursors support the iterator
  protocol. While equivalent to ``cursor.read(1)[0]``, `StopIteration` is raised
  if the returned sequence is empty. (``__next__()``)

 ``Cursor.close()``
  For cursors opened using ``cursor_from_id()``, this method must be called in
  order to ``CLOSE`` the cursor. For cursors created by invoking a prepared
  statement, this is not necessary as the garbage collection interface will take
  the appropriate steps.

 ``Cursor.clone()``
  Create a new cursor object based on the same factors that were used to
  create ``c``.

 ``Cursor.msghook(msg)``
  By default, the `msghook` attribute does not exist. If set to a callable, any
  message that occurs during an operation of the cursor will be given to the
  callable. See the `Database Messages`_ section for more information.


Cursors have some additional configuration properties that may be modified
during the use of the cursor:

 ``Cursor.direction``
  A value of `True`, the default, will cause read to fetch forwards, whereas a
  value of `False` will cause it to fetch backwards. ``'BACKWARD'`` and
  ``'FORWARD'`` can be used instead of `False` and `True`.

Cursors normally share metadata with the statements that create them, so it is
usually unnecessary for referencing the cursor's column descriptions directly.
However, when a cursor is opened from an identifier, the cursor interface must
collect the metadata itself. These attributes provide the metadata in absence of
a statement object:

 ``Cursor.sql_column_types``
  A sequence of SQL type names specifying the types of the columns produced by
  the cursor. `None` if the cursor does not return row-data.

 ``Cursor.pg_column_types``
  A sequence of PostgreSQL type Oid's specifying the types of the columns produced by
  the cursor. `None` if the cursor does not return row-data.

 ``Cursor.column_types``
  A sequence of Python types that the cursor will produce.

 ``Cursor.column_names``
  A sequence of `str` objects specifying the names of the columns produced by
  the cursor. `None` if the cursor does not return row-data.

 ``Cursor.statement``
  The statement that was executed that created the cursor. `None` if
  unknown--``db.cursor_from_id()``.


Scrollable Cursors
------------------

Scrollable cursors are supported for applications that need to implement paging.
When statements are invoked via the ``declare(...)`` method, the returned cursor
is scrollable.

.. note::
 Scrollable cursors never pre-fetch in order to provide guaranteed positioning.

The cursor interface supports scrolling using the ``seek`` method. Like
``read``, it is semantically similar to a file object's ``seek()``.

``seek`` takes two arguments: ``position`` and ``whence``:

 ``position``
  The position to scroll to. The meaning of this is determined by ``whence``.

 ``whence``
  How to use the position: absolute, relative, or absolute from end:

   absolute: ``'ABSOLUTE'`` or ``0`` (default)
    seek to the absolute position in the cursor relative to the beginning of the
    cursor.

   relative: ``'RELATIVE'`` or ``1``
    seek to the relative position. Negative ``position``'s will cause a MOVE
    backwards, while positive ``position``'s will MOVE forwards.

   from end: ``'FROM_END'`` or ``2``
    seek to the end of the cursor and then MOVE backwards by the given
    ``position``.

The ``whence`` keyword argument allows for either numeric and textual
specifications.

Scrolling through employees::

	>>> emps_by_age = db.prepare("""
	... SELECT
	... 	employee_name, employee_salary, employee_dob, employee_hire_date,
	... 	EXTRACT(years FROM AGE(employee_dob)) AS age
	... ORDER BY age ASC
	... """)
	>>> c = emps_by_age.declare()
	>>> # seek to the end, ``2`` works as well.
	>>> c.seek(0, 'FROM_END')
	>>> # scroll back one, ``1`` works as well.
	>>> c.seek(-1, 'RELATIVE')
	>>> # and back to the beginning again
	>>> c.seek(0)

Additionally, scrollable cursors support backward fetches by specifying the
direction keyword argument::

	>>> c.seek(0, 2)
	>>> c.read(1, 'BACKWARD')


Cursor Direction
----------------

The ``direction`` property on the cursor states the default direction for read
and seek operations. Normally, the direction is `True`, ``'FORWARD'``. When the
property is set to ``'BACKWARD'`` or `False`, the read method will fetch
backward by default, and seek operations will be inverted to simulate a
reversely ordered cursor. The following example illustrates the effect::

	>>> reverse_c = db.prepare('SELECT i FROM generate_series(99, 0, -1) AS g(i)').declare()
	>>> c = db.prepare('SELECT i FROM generate_series(0, 99) AS g(i)').declare()
	>>> reverse_c.direction = 'BACKWARD'
	>>> reverse_c.seek(0)
	>>> c.read() == reverse_c.read()

Furthermore, when the cursor is configured to read backwards, specifying
``'BACKWARD'`` for read's ``direction`` argument will ultimately cause a forward
fetch. This potentially confusing facet of direction configuration is
implemented in order to create an appropriate symmetry in functionality.
The cursors in the above example contain the same rows, but are ultimately in
reverse order. The backward direction property is designed so that the effect
of any read or seek operation on those cursors is the same::

	>>> reverse_c.seek(50)
	>>> c.seek(50)
	>>> c.read(10) == reverse_c.read(10)
	>>> c.read(10, 'BACKWARD') == reverse_c.read(10, 'BACKWARD')

And for relative seeks::

	>>> c.seek(-10, 1)
	>>> reverse_c.seek(-10, 1)
	>>> c.read(10, 'BACKWARD') == reverse_c.read(10, 'BACKWARD')


Rows
====

Rows received from PostgreSQL are instantiated into `postgresql.types.Row`
objects. Rows are both a sequence and a mapping. Items accessed with an `int`
are seen as indexes and other objects are seen as keys::

	>>> row = db.prepare("SELECT 't'::text AS col0, 2::int4 AS col1").first()
	>>> row
	('t', 2)
	>>> row[0]
	't'
	>>> row["col0"]
	't'

However, this extra functionality is not free. The cost of instantiating
`postgresql.types.Row` objects is quite measurable, so the `chunks()` execution
method will produce `builtins.tuple` objects for cases where performance is
critical.

.. note::
 Attributes aren't used to provide access to values due to potential conflicts
 with existing method and property names.


Row Interface Points
--------------------

Rows implement the `collections.Mapping` and `collections.Sequence` interfaces.

 ``Row.keys()``
  An iterable producing the column names. Order is not guaranteed. See the
  ``column_names`` property to get an ordered sequence.

 ``Row.values()``
  Iterable to the values in the row.

 ``Row.get(key_or_index[, default=None])``
  Get the item in the row. If the key doesn't exist or the index is out of
  range, return the default.

 ``Row.items()``
  Iterable of key-value pairs. Ordered by index.

 ``iter(Row)``
  Iterable to the values in index order.

 ``value in Row``
  Whether or not the value exists in the row. (__contains__)

 ``Row[key_or_index]``
  If ``key_or_index`` is an integer, return the value at that index. If the
  index is out of range, raise an `IndexError`. Otherwise, return the value
  associated with column name. If the given key, ``key_or_index``, does not
  exist, raise a `KeyError`.

 ``Row.index_from_key(key)``
  Return the index associated with the given key.

 ``Row.key_from_index(index)``
  Return the key associated with the given index.

 ``Row.transform(*args, **kw)``
  Create a new row object of the same length, with the same keys, but with new
  values produced by applying the given callables to the corresponding items.
  Callables given as ``args`` will be associated with values by their index and
  callables given as keywords will be associated with values by their key,
  column name.

While the mapping interfaces will provide most of the needed information, some
additional properties are provided for consistency with statement and cursor
objects.

 ``Row.column_names``
  Property providing an ordered sequence of column names. The index corresponds
  to the row value-index that the name refers to.

  	>>> row[row.column_names[i]] == row[i]


Row Transformations
-------------------

After a row is returned, sometimes the data in the row is not in the desired
format. Further processing is needed if the row object is to going to be
given to another piece of code which requires an object of differring
consistency.

The ``transform`` method on row objects provides a means to create a new row
object consisting of the old row's items, but with certain columns transformed
using the given callables::

	>>> row = db.prepare("""
	...  SELECT
	...   'XX9301423'::text AS product_code,
	...   2::int4 AS quantity,
	...   '4.92'::numeric AS total
	... """).first()
	>>> row
	('XX9301423', 2, Decimal("4.92"))
	>>> row.transform(quantity = str)
	('XX9301423', '2', Decimal("4.92"))

``transform`` supports both positional and keyword arguments in order to
assign the callable for a column's transformation::

	>>> from operator import methodcaller
	>>> row.transform(methodcaller('strip', 'XX'))
	('9301423', 2, Decimal("4.92"))

Of course, more than one column can be transformed::

	>>> stripxx = methodcaller('strip', 'XX')
	>>> row.transform(stripxx, str, str)
	('9301423', '2', '4.92')

`None` can also be used to indicate no transformation::

	>>> row.transform(None, str, str)
	('XX9301423', '2', '4.92')

More advanced usage can make use of lambdas for compound transformations in a
single pass of the row::

	>>> strip_and_int = lambda x: int(stripxx(x))
	>>> row.transform(strip_and_int)
	(9301423, 2, Decimal("4.92"))

Transformations will be, more often than not, applied against *rows* as
opposed to *a* row. Using `operator.methodcaller` with `map` provides the
necessary functionality to create simple iterables producing transformed row
sequences::

	>>> import decimal
	>>> apply_tax = lambda x: (x * decimal.Decimal("0.1")) + x
	>>> transform_row = methodcaller('transform', strip_and_int, None, apply_tax)
	>>> r = map(transform_row, [row])
	>>> list(r)
	[(9301423, 2, Decimal('5.412'))]

And finally, `functools.partial` can be used to create a simple callable::

	>>> from functools import partial
	>>> transform_rows = partial(map, transform_row)
	>>> list(transform_rows([row]))
	[(9301423, 2, Decimal('5.412'))]


Queries
=======

Queries in `py-postgresql` are single use prepared statements. They exist primarily for
syntactic convenience, but they also allow the driver to recognize the short lifetime of
the statement.

Single use statements are supported using the ``query`` property on connection
objects, :py:class:`postgresql.api.Connection.query`. The statement object is not
available when using queries as the results, or handle to the results, are directly returned.

Queries have access to all execution methods:

 * ``Connection.query(sql, *parameters)``
 * ``Connection.query.rows(sql, *parameters)``
 * ``Connection.query.column(sql, *parameters)``
 * ``Connection.query.first(sql, *parameters)``
 * ``Connection.query.chunks(sql, *parameters)``
 * ``Connection.query.declare(sql, *parameters)``
 * ``Connection.query.load_rows(sql, collections.Iterable(parameters))``
 * ``Connection.query.load_chunks(collections.Iterable(collections.Iterable(parameters)))``

In cases where a sequence of one-shot queries needs to be performed, it may be important to
avoid unnecessary repeat attribute resolution from the connection object as the ``query``
property is an interface object created on access. Caching the target execution methods is
recommended::

   qrows = db.query.rows
   l = []
   for x in my_queries:
      l.append(qrows(x))

The characteristic of Each execution method is discussed in the prior
`Prepared Statements`_ section.

Stored Procedures
=================

The ``proc`` method on `postgresql.api.Database` objects provides a means to
create a reference to a stored procedure on the remote database.
`postgresql.api.StoredProcedure` objects are used to represent the referenced
SQL routine.

This provides a direct interface to functions stored on the database. It
leverages knowledge of the parameters and results of the function in order
to provide the user with a natural interface to the procedure::

	>>> func = db.proc('version()')
	>>> func()
	'PostgreSQL 8.3.6 on ...'


Stored Procedure Interface Points
---------------------------------

It's more-or-less a function, so there's only one interface point:

 ``func(*args, **kw)`` (``__call__``)
  Stored procedure objects are callable, executing a procedure will return an
  object of suitable representation for a given procedure's type signature.

  If it returns a single object, it will return the single object produced by
  the procedure.

  If it's a set returning function, it will return an *iterable* to the values
  produced by the procedure.

  In cases of set returning function with multiple OUT-parameters, a cursor
  will be returned.


Stored Procedure Type Support
-----------------------------

Stored procedures support most types of functions. "Function Types" being set
returning functions, multiple-OUT parameters, and simple single-object returns.

Set-returning functions, SRFs return a sequence::

	>>> generate_series = db.proc('generate_series(int,int)')
	>>> gs = generate_series(1, 20)
	>>> gs
	<generator object <genexpr>>
	>>> next(gs)
	1
	>>> list(gs)
	[2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

For functions like ``generate_series()``, the driver is able to identify that
the return is a sequence of *solitary* integer objects, so the result of the
function is just that, a sequence of integers.

Functions returning composite types are recognized, and return row objects::

	>>> db.execute("""
	... CREATE FUNCTION composite(OUT i int, OUT t text)
	... LANGUAGE SQL AS
	... $body$
	...  SELECT 900::int AS i, 'sample text'::text AS t;
	... $body$;
	... """)
	>>> composite = db.proc('composite()')
	>>> r = composite()
	>>> r
	(900, 'sample text')
	>>> r['i']
	900
	>>> r['t']
	'sample text'

Functions returning a set of composites are recognized, and the result is a
`postgresql.api.Cursor` object whose column names are consistent with the names
of the OUT parameters::

	>>> db.execute("""
	... CREATE FUNCTION srfcomposite(out i int, out t text)
	... RETURNS SETOF RECORD
	... LANGUAGE SQL AS
	... $body$
	...  SELECT 900::int AS i, 'sample text'::text AS t
	...  UNION ALL
	...  SELECT 450::int AS i, 'more sample text'::text AS t
	... $body$;
	... """)
	>>> srfcomposite = db.proc('srfcomposite()')
	>>> r = srfcomposite()
	>>> next(r)
	(900, 'sample text')
	>>> v = next(r)
	>>> v['i'], v['t']
	(450, 'more sample text')


Transactions
============

Transactions are managed by creating an object corresponding to a
transaction started on the server. A transaction is a transaction block,
a savepoint, or a prepared transaction. The ``xact(...)`` method on the
connection object provides the standard method for creating a
`postgresql.api.Transaction` object to manage a transaction on the connection.

The creation of a transaction object does not start the transaction. Rather, the
transaction must be explicitly started using the ``start()`` method on the
transaction object. Usually, transactions *should* be managed with the context
manager interfaces::

	>>> with db.xact():
	...  ...

The transaction in the above example is opened, started, by the ``__enter__``
method invoked by the with-statement's usage. It will be subsequently
committed or rolled-back depending on the exception state and the error state
of the connection when ``__exit__`` is called.

**Using the with-statement syntax for managing transactions is strongly
recommended.** By using the transaction's context manager, it allows for Python
exceptions to be properly treated as fatal to the transaction as when an
uncaught exception of any kind occurs within the block, it is unlikely that
the state of the transaction can be trusted. Additionally, the ``__exit__``
method provides a safe-guard against invalid commits. This can occur if a
database error is inappropriately caught within a block without being raised.

The context manager interfaces are higher level interfaces to the explicit
instruction methods provided by `postgresql.api.Transaction` objects.


Transaction Configuration
-------------------------

Keyword arguments given to ``xact()`` provide the means for configuring the
properties of the transaction. Only three points of configuration are available:

 ``isolation``
  The isolation level of the transaction. This must be a string. It will be
  interpolated directly into the START TRANSACTION statement. Normally,
  'SERIALIZABLE' or 'READ COMMITTED':

  	>>> with db.xact('SERIALIZABLE'):
  	...  ...

 ``mode``
  A string, 'READ ONLY' or 'READ WRITE'. States the mutability of stored
  information in the database. Like ``isolation``, this is interpolated
  directly into the START TRANSACTION string.

The specification of any of these transaction properties imply that the transaction
is a block. Savepoints do not take configuration, so if a transaction identified
as a block is started while another block is running, an exception will be
raised.


Transaction Interface Points
----------------------------

The methods available on transaction objects manage the state of the transaction
and relay any necessary instructions to the remote server in order to reflect
that change of state.

	>>> x = db.xact(...)

 ``x.start()``
  Start the transaction.

 ``x.commit()``
  Commit the transaction.

 ``x.rollback()``
  Abort the transaction.

These methods are primarily provided for applications that manage transactions
in a way that cannot be formed around single, sequential blocks of code.
Generally, using these methods require additional work to be performed by the
code that is managing the transaction.
If usage of these direct, instructional methods is necessary, it is important to
note that if the database is in an error state when a *transaction block's*
commit() is executed, an implicit rollback will occur. The transaction object
will simply follow instructions and issue the ``COMMIT`` statement, and it will
succeed without exception.


Error Control
-------------

Handling *database* errors inside transaction CMs is generally discouraged as
any database operation that occurs within a failed transaction is an error
itself. It is important to trap any recoverable database errors *outside* of the
scope of the transaction's context manager:

	>>> try:
	...  with db.xact():
	...   ...
	... except postgresql.exceptions.UniqueError:
	...  pass

In cases where the database is in an error state, but the context exits
without an exception, a `postgresql.exceptions.InFailedTransactionError` is
raised by the driver:

	>>> with db.xact():
	...  try:
	...   ...
	...  except postgresql.exceptions.UniqueError:
	...   pass
	...
	Traceback (most recent call last):
	 ...
	postgresql.exceptions.InFailedTransactionError: invalid block exit detected
	CODE: 25P02
	SEVERITY: ERROR

Normally, if a ``COMMIT`` is issued on a failed transaction, the command implies a
``ROLLBACK`` without error. This is a very undesirable result for the CM's exit
as it may allow for code to be ran that presumes the transaction was committed.
The driver intervenes here and raises the
`postgresql.exceptions.InFailedTransactionError` to safe-guard against such
cases. This effect is consistent with savepoint releases that occur during an
error state. The distinction between the two cases is made using the ``source``
property on the raised exception.


Settings
========

SQL's SHOW and SET provides a means to configure runtime parameters on the
database("GUC"s). In order to save the user some grief, a
`collections.MutableMapping` interface is provided to simplify configuration.

The ``settings`` attribute on the connection provides the interface extension.

The standard dictionary interface is supported:

	>>> db.settings['search_path'] = "$user,public"

And ``update(...)`` is better performing for multiple sets:

	>>> db.settings.update({
	...  'search_path' : "$user,public",
	...  'default_statistics_target' : "1000"
	... })

.. note::
 The ``transaction_isolation`` setting cannot be set using the ``settings``
 mapping. Internally, ``settings`` uses ``set_config``, which cannot adjust
 that particular setting.

Settings Interface Points
-------------------------

Manipulation and interrogation of the connection's settings is achieved by
using the standard `collections.MutableMapping` interfaces.

 ``Connection.settings[k]``
  Get the value of a single setting.

 ``Connection.settings[k] = v``
  Set the value of a single setting.

 ``Connection.settings.update([(k1,v2), (k2,v2), ..., (kn,vn)])``
  Set multiple settings using a sequence of key-value pairs.

 ``Connection.settings.update({k1 : v1, k2 : v2, ..., kn : vn})``
  Set multiple settings using a dictionary or mapping object.

 ``Connection.settings.getset([k1, k2, ..., kn])``
  Get a set of a settings. This is the most efficient way to get multiple
  settings as it uses a single request.

 ``Connection.settings.keys()``
  Get all available setting names.

 ``Connection.settings.values()``
  Get all setting values.

 ``Connection.settings.items()``
  Get a sequence of key-value pairs corresponding to all settings on the
  database.

Settings Management
-------------------

`postgresql.api.Settings` objects can create context managers when called.
This gives the user with the ability to specify sections of code that are to
be ran with certain settings. The settings' context manager takes full
advantage of keyword arguments in order to configure the context manager:

	>>> with db.settings(search_path = 'local,public', timezone = 'mst'):
	...  ...

`postgresql.api.Settings` objects are callable; the return is a context manager
configured with the given keyword arguments representing the settings to use for
the block of code that is about to be executed.

When the block exits, the settings will be restored to the values that they had
before the block entered.


Type Support
============

The driver supports a large number of PostgreSQL types at the binary level.
Most types are converted to standard Python types. The remaining types are
usually PostgreSQL specific types that are converted into objects whose class
is defined in `postgresql.types`.

When a conversion function is not available for a particular type, the driver
will use the string format of the type and instantiate a `str` object
for the data. It will also expect `str` data when parameter of a type without a
conversion function is bound.


.. note::
   Generally, these standard types are provided for convenience. If conversions into
   these datatypes are not desired, it is recommended that explicit casts into
   ``text`` are made in statement string.


.. table:: Python types used to represent PostgreSQL types.

 ================================= ================================== ===========
 PostgreSQL Types                  Python Types                       SQL Types
 ================================= ================================== ===========
 `postgresql.types.INT2OID`        `int`                              smallint
 `postgresql.types.INT4OID`        `int`                              integer
 `postgresql.types.INT8OID`        `int`                              bigint
 `postgresql.types.FLOAT4OID`      `float`                            float
 `postgresql.types.FLOAT8OID`      `float`                            double
 `postgresql.types.VARCHAROID`     `str`                              varchar
 `postgresql.types.BPCHAROID`      `str`                              char
 `postgresql.types.XMLOID`         `xml.etree` (cElementTree)         xml

 `postgresql.types.DATEOID`        `datetime.date`                    date
 `postgresql.types.TIMESTAMPOID`   `datetime.datetime`                timestamp
 `postgresql.types.TIMESTAMPTZOID` `datetime.datetime` (tzinfo)       timestamptz
 `postgresql.types.TIMEOID`        `datetime.time`                    time
 `postgresql.types.TIMETZOID`      `datetime.time`                    timetz
 `postgresql.types.INTERVALOID`    `datetime.timedelta`               interval

 `postgresql.types.NUMERICOID`     `decimal.Decimal`                  numeric
 `postgresql.types.BYTEAOID`       `bytes`                            bytea
 `postgresql.types.TEXTOID`        `str`                              text
 <contrib_hstore>                  `dict`                             hstore
 ================================= ================================== ===========

The mapping in the above table *normally* goes both ways. So when a parameter
is passed to a statement, the type *should* be consistent with the corresponding
Python type. However, many times, for convenience, the object will be passed
through the type's constructor, so it is not always necessary.


Arrays
------

Arrays of PostgreSQL types are supported with near transparency. For simple
arrays, arbitrary iterables can just be given as a statement's parameter and the
array's constructor will consume the objects produced by the iterator into a
`postgresql.types.Array` instance. However, in situations where the array has
multiple dimensions, `list` objects are used to delimit the boundaries of the
array.

	>>> ps = db.prepare("select $1::int[]")
	>>> ps.first([(1,2), (2,3)])
	Traceback:
	 ...
	postgresql.exceptions.ParameterError

In the above case, it is apparent that this array is supposed to have two
dimensions. However, this is not the case for other types:

	>>> ps = db.prepare("select $1::point[]")
	>>> ps.first([(1,2), (2,3)])
	postgresql.types.Array([postgresql.types.point((1.0, 2.0)), postgresql.types.point((2.0, 3.0))])

Lists are used to provide the necessary boundary information:

	>>> ps = db.prepare("select $1::int[]")
	>>> ps.first([[1,2],[2,3]])
	postgresql.types.Array([[1,2],[2,3]])

The above is the appropriate way to define the array from the original example.

.. hint::
 The root-iterable object given as an array parameter does not need to be a
 list-type as it's assumed to be made up of elements.


Composites
----------

Composites are supported using `postgresql.types.Row` objects to represent
the data. When a composite is referenced for the first time, the driver
queries the database for information about the columns that make up the type.
This information is then used to create the necessary I/O routines for packing
and unpacking the parameters and columns of that type::

	>>> db.execute("CREATE TYPE ctest AS (i int, t text, n numeric);")
	>>> ps = db.prepare("SELECT $1::ctest")
	>>> i = (100, 'text', "100.02013")
	>>> r = ps.first(i)
	>>> r["t"]
	'text'
	>>> r["n"]
	Decimal("100.02013")

Or if use of a dictionary is desired::

	>>> r = ps.first({'t' : 'just-the-text'})
	>>> r
	(None, 'just-the-text', None)

When a dictionary is given to construct the row, absent values are filled with
`None`.

.. _db_messages:

Database Messages
=================

By default, py-postgresql gives detailed reports of messages emitted by the
database. Often, the verbosity is excessive due to single target processes or
existing application infrastructure for tracing the sources of various events.
Normally, this verbosity is not a significant problem as the driver defaults the
``client_min_messages`` setting to ``'WARNING'`` by default.

However, if ``NOTICE`` or ``INFO`` messages are needed, finer grained control
over message propagation may be desired, py-postgresql's object relationship
model provides a common protocol for controlling message propagation and,
ultimately, display.

The ``msghook`` attribute on elements--for instance, Statements, Connections,
and Connectors--is absent by default. However, when present on an object that
contributed the cause of a message event, it will be invoked with the Message,
`postgresql.message.Message`, object as its sole parameter. The attribute of
the object that is closest to the event is checked first, if present it will
be called. If the ``msghook()`` call returns a `True`
value(specficially, ``bool(x) is True``), the message will *not* be
propagated any further. However, if a `False` value--notably, `None`--is
returned, the next element is checked until the list is exhausted and the
message is given to `postgresql.sys.msghook`. The normal list of elements is
as follows::

	Output → Statement → Connection → Connector → Driver → postgresql.sys

Where ``Output`` can be a `postgresql.api.Cursor` object produced by
``declare(...)`` or an implicit output management object used *internally* by
``Statement.__call__()`` and other statement execution methods. Setting the
``msghook`` attribute on `postgresql.api.Statement` gives very fine
control over raised messages. Consider filtering the notice message on create
table statements that implicitly create indexes::

	>>> db = postgresql.open(...)
	>>> db.settings['client_min_messages'] = 'NOTICE'
	>>> ct_this = db.prepare('CREATE TEMP TABLE "this" (i int PRIMARY KEY)')
	>>> ct_that = db.prepare('CREATE TEMP TABLE "that" (i int PRIMARY KEY)')
	>>> def filter_notices(msg):
	...  if msg.details['severity'] == 'NOTICE':
	...   return True
	...
	>>> ct_that()
	NOTICE: CREATE TABLE / PRIMARY KEY will create implicit index "that_pkey" for table "that"
	...
	('CREATE TABLE', None)
	>>> ct_this.msghook = filter_notices
	>>> ct_this()
	('CREATE TABLE', None)
	>>>

The above illustrates the quality of an installed ``msghook`` that simply
inhibits further propagation of messages with a severity of 'NOTICE'--but, only
notices coming from objects derived from the ``ct_this``
`postgresql.api.Statement` object.

Subsequently, if the filter is installed on the connection's ``msghook``::

	>>> db = postgresql.open(...)
	>>> db.settings['client_min_messages'] = 'NOTICE'
	>>> ct_this = db.prepare('CREATE TEMP TABLE "this" (i int PRIMARY KEY)')
	>>> ct_that = db.prepare('CREATE TEMP TABLE "that" (i int PRIMARY KEY)')
	>>> def filter_notices(msg):
	...  if msg.details['severity'] == 'NOTICE':
	...   return True
	...
	>>> db.msghook = filter_notices
	>>> ct_that()
	('CREATE TABLE', None)
	>>> ct_this()
	('CREATE TABLE', None)
	>>>

Any message with ``'NOTICE'`` severity coming from the connection, ``db``, will be
suffocated by the ``filter_notices`` function. However, if a ``msghook`` is
installed on either of those statements, it would be possible for display to
occur depending on the implementation of the hook installed on the statement
objects.


Message Metadata
----------------

PostgreSQL messages, `postgresql.message.Message`, are primarily described in three
parts: the SQL-state code, the main message string, and a mapping containing the
details. The follow attributes are available on message objects:

 ``Message.message``
  The primary message string.

 ``Message.code``
  The SQL-state code associated with a given message.

 ``Message.source``
  The origins of the message. Normally, ``'SERVER'`` or ``'CLIENT'``.

 ``Message.location``
  A terse, textual representation of ``'file'``, ``'line'``, and ``'function'``
  provided by the associated ``details``.

 ``Message.details``
  A mapping providing extended information about a message. This mapping
  object **can** contain the following keys:

   ``'severity'``
    Any of ``'DEBUG'``, ``'INFO'``, ``'NOTICE'``, ``'WARNING'``, ``'ERROR'``,
    ``'FATAL'``, or ``'PANIC'``; the latter three are usually associated with a
    `postgresql.exceptions.Error` instance.

   ``'context'``
    The CONTEXT portion of the message.

   ``'detail'``
    The DETAIL portion of the message.

   ``'hint'``
    The HINT portion of the message.

   ``'position'``
    A number identifying the position in the statement string that caused a
    parse error.

   ``'file'``
    The name of the file that emitted the message.
    (*normally* server information)

   ``'function'``
    The name of the function that emitted the message.
    (*normally* server information)

   ``'line'``
    The line of the file that emitted the message.
    (*normally* server information)
