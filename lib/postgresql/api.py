##
# .api - ABCs for database interface elements
##
"""
Application Programmer Interfaces for PostgreSQL.

``postgresql.api`` is a collection of Python APIs for the PostgreSQL DBMS. It
is designed to take full advantage of PostgreSQL's features to provide the
Python programmer with substantial convenience.

This module is used to define "PG-API". It creates a set of ABCs
that makes up the basic interfaces used to work with a PostgreSQL server.
"""
import collections
import abc

from .python.element import Element

__all__ = [
	'Message',
	'Statement',
	'Chunks',
	'Cursor',
	'Connector',
	'Category',
	'Database',
	'TypeIO',
	'Connection',
	'Transaction',
	'Settings',
	'StoredProcedure',
	'Driver',
	'Installation',
	'Cluster',
]

class Message(Element):
	"""
	A message emitted by PostgreSQL.
	A message being a NOTICE, WARNING, INFO, etc.
	"""
	_e_label = 'MESSAGE'

	severities = (
		'DEBUG',
		'INFO',
		'NOTICE',
		'WARNING',
		'ERROR',
		'FATAL',
		'PANIC',
	)
	sources = (
		'SERVER',
		'CLIENT',
	)

	@property
	@abc.abstractmethod
	def source(self) -> str:
		"""
		Where the message originated from. Normally, 'SERVER', but sometimes
		'CLIENT'.
		"""

	@property
	@abc.abstractmethod
	def code(self) -> str:
		"""
		The SQL state code of the message.
		"""

	@property
	@abc.abstractmethod
	def message(self) -> str:
		"""
		The primary message string.
		"""

	@property
	@abc.abstractmethod
	def details(self) -> dict:
		"""
		The additional details given with the message. Common keys *should* be the
		following:

		 * 'severity'
		 * 'context'
		 * 'detail'
		 * 'hint'
		 * 'file'
		 * 'line'
		 * 'function'
		 * 'position'
		 * 'internal_position'
		 * 'internal_query'
		"""

	@abc.abstractmethod
	def isconsistent(self, other) -> bool:
		"""
		Whether the fields of the `other` Message object is consistent with the
		fields of `self`.

		This *must* return the result of the comparison of code, source, message,
		and details.

		This method is provided as the alternative to overriding equality;
		often, pointer equality is the desirable means for comparison, but
		equality of the fields is also necessary.
		"""

class Result(Element):
	"""
	A result is an object managing the results of a prepared statement.

	These objects represent a binding of parameters to a given statement object.

	For results that were constructed on the server and a reference passed back
	to the client, statement and parameters may be None.
	"""
	_e_label = 'RESULT'
	_e_factors = ('statement', 'parameters', 'cursor_id')

	@abc.abstractmethod
	def close(self) -> None:
		"""
		Close the Result handle.
		"""

	@property
	@abc.abstractmethod
	def cursor_id(self) -> str:
		"""
		The cursor's identifier.
		"""

	@property
	@abc.abstractmethod
	def sql_column_types(self) -> [str]:
		"""
		The type of the columns produced by the cursor.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'CHARACTER VARYING', 'INTERVAL']
		"""

	@property
	@abc.abstractmethod
	def pg_column_types(self) -> [int]:
		"""
		The type Oids of the columns produced by the cursor.

		A sequence of `int` objects stating the SQL type name::

			[27, 28]
		"""

	@property
	@abc.abstractmethod
	def column_names(self) -> [str]:
		"""
		The attribute names of the columns produced by the cursor.

		A sequence of `str` objects stating the column name::

			['column1', 'column2', 'emp_name']
		"""

	@property
	@abc.abstractmethod
	def column_types(self) -> [str]:
		"""
		The Python types of the columns produced by the cursor.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@property
	@abc.abstractmethod
	def parameters(self) -> (tuple, None):
		"""
		The parameters bound to the cursor. `None`, if unknown and an empty tuple
		`()`, if no parameters were given.

		These should be the *original* parameters given to the invoked statement.

		This should only be `None` when the cursor is created from an identifier,
		`postgresql.api.Database.cursor_from_id`.
		"""

	@property
	@abc.abstractmethod
	def statement(self) -> ("Statement", None):
		"""
		The query object used to create the cursor. `None`, if unknown.

		This should only be `None` when the cursor is created from an identifier,
		`postgresql.api.Database.cursor_from_id`.
		"""

class Chunks(
	Result,
	collections.Iterator,
	collections.Iterable,
):
	pass

class Cursor(
	Result,
	collections.Iterator,
	collections.Iterable,
):
	"""
	A `Cursor` object is an interface to a sequence of tuples(rows). A result
	set. Cursors publish a file-like interface for reading tuples from a cursor
	declared on the database.

	`Cursor` objects are created by invoking the `Statement.declare`
	method or by opening a cursor using an identifier via the
	`Database.cursor_from_id` method.
	"""
	_e_label = 'CURSOR'

	_seek_whence_map = {
		0 : 'ABSOLUTE',
		1 : 'RELATIVE',
		2 : 'FROM_END',
		3 : 'FORWARD',
		4 : 'BACKWARD'
	}
	_direction_map = {
		True : 'FORWARD',
		False : 'BACKWARD',
	}

	@abc.abstractmethod
	def clone(self) -> "Cursor":
		"""
		Create a new cursor using the same factors as `self`.
		"""

	def __iter__(self):
		return self

	@property
	@abc.abstractmethod
	def direction(self) -> bool:
		"""
		The default `direction` argument for read().

		When `True` reads are FORWARD.
		When `False` reads are BACKWARD.

		Cursor operation option.
		"""

	@abc.abstractmethod
	def read(self,
		quantity : "Number of rows to read" = None,
		direction : "Direction to fetch in, defaults to `self.direction`" = None,
	) -> ["Row"]:
		"""
		Read, fetch, the specified number of rows and return them in a list.
		If quantity is `None`, all records will be fetched.

		`direction` can be used to override the default configured direction.

		This alters the cursor's position.

		Read does not directly correlate to FETCH. If zero is given as the
		quantity, an empty sequence *must* be returned.
		"""

	@abc.abstractmethod
	def __next__(self) -> "Row":
		"""
		Get the next tuple in the cursor.
		Advances the cursor position by one.
		"""

	@abc.abstractmethod
	def seek(self, offset, whence = 'ABSOLUTE'):
		"""
		Set the cursor's position to the given offset with respect to the
		whence parameter and the configured direction.

		Whence values:

		 ``0`` or ``"ABSOLUTE"``
		  Absolute.
		 ``1`` or ``"RELATIVE"``
		  Relative.
		 ``2`` or ``"FROM_END"``
		  Absolute from end.
		 ``3`` or ``"FORWARD"``
		  Relative forward.
		 ``4`` or ``"BACKWARD"``
		  Relative backward.

		Direction effects whence. If direction is BACKWARD, ABSOLUTE positioning
		will effectively be FROM_END, RELATIVE's position will be negated, and
		FROM_END will effectively be ABSOLUTE.
		"""

class Execution(metaclass = abc.ABCMeta):
	"""
	The abstract class of execution methods.
	"""

	@abc.abstractmethod
	def __call__(self, *parameters : "Positional Parameters") -> ["Row"]:
		"""
		Execute the prepared statement with the given arguments as parameters.

		Usage:

		>>> p=db.prepare("SELECT column FROM ttable WHERE key = $1")
		>>> p('identifier')
		[...]
		"""

	@abc.abstractmethod
	def column(self, *parameters) -> collections.Iterable:
		"""
		Return an iterator producing the values of first column of the
		rows produced by the cursor created from the statement bound with the
		given parameters.

		Column iterators are never scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction to
		allow cross-transaction access.

		`column` is designed for the situations involving large data sets.

		Each iteration returns a single value.

		column expressed in sibling terms::

			return map(operator.itemgetter(0), self.rows(*parameters))
		"""

	@abc.abstractmethod
	def chunks(self, *parameters) -> collections.Iterable:
		"""
		Return an iterator producing sequences of rows produced by the cursor
		created from the statement bound with the given parameters.

		Chunking iterators are *never* scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction.

		`chunks` is designed for moving large data sets efficiently.

		Each iteration returns sequences of rows *normally* of length(seq) ==
		chunksize. If chunksize is unspecified, a default, positive integer will
		be filled in. The rows contained in the sequences are only required to
		support the basic `collections.Sequence` interfaces; simple and quick
		sequence types should be used.
		"""

	@abc.abstractmethod
	def rows(self, *parameters) -> collections.Iterable:
		"""
		Return an iterator producing rows produced by the cursor
		created from the statement bound with the given parameters.

		Row iterators are never scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction to
		allow cross-transaction access.

		`rows` is designed for the situations involving large data sets.

		Each iteration returns a single row. Arguably, best implemented::

			return itertools.chain.from_iterable(self.chunks(*parameters))
		"""

	@abc.abstractmethod
	def column(self, *parameters) -> collections.Iterable:
		"""
		Return an iterator producing the values of the first column in
		the cursor created from the statement bound with the given parameters.

		Column iterators are never scrollable.

		Supporting cursors will be WITH HOLD when outside of a transaction to
		allow cross-transaction access.

		`column` is designed for the situations involving large data sets.

		Each iteration returns a single value. `column` is equivalent to::

			return map(operator.itemgetter(0), self.rows(*parameters))
		"""

	@abc.abstractmethod
	def declare(self, *parameters) -> Cursor:
		"""
		Return a scrollable cursor with hold using the statement bound with the
		given parameters.
		"""

	@abc.abstractmethod
	def first(self, *parameters) -> "'First' object that is returned by the query":
		"""
		Execute the prepared statement with the given arguments as parameters.
		If the statement returns rows with multiple columns, return the first
		row. If the statement returns rows with a single column, return the
		first column in the first row. If the query does not return rows at all,
		return the count or `None` if no count exists in the completion message.

		Usage:

		>>> db.prepare("SELECT * FROM ttable WHERE key = $1").first("somekey")
		('somekey', 'somevalue')
		>>> db.prepare("SELECT 'foo'").first()
		'foo'
		>>> db.prepare("INSERT INTO atable (col) VALUES (1)").first()
		1
		"""

	@abc.abstractmethod
	def load_rows(self,
		iterable : "A iterable of tuples to execute the statement with"
	):
		"""
		Given an iterable, `iterable`, feed the produced parameters to the
		query. This is a bulk-loading interface for parameterized queries.

		Effectively, it is equivalent to:

			>>> q = db.prepare(sql)
			>>> for i in iterable:
			...  q(*i)

		Its purpose is to allow the implementation to take advantage of the
		knowledge that a series of parameters are to be loaded so that the
		operation can be optimized.
		"""

	@abc.abstractmethod
	def load_chunks(self,
		iterable : "A iterable of chunks of tuples to execute the statement with"
	):
		"""
		Given an iterable, `iterable`, feed the produced parameters of the chunks
		produced by the iterable to the query. This is a bulk-loading interface
		for parameterized queries.

		Effectively, it is equivalent to:

			>>> ps = db.prepare(...)
			>>> for c in iterable:
			...  for i in c:
			...   q(*i)

		Its purpose is to allow the implementation to take advantage of the
		knowledge that a series of chunks of parameters are to be loaded so
		that the operation can be optimized.
		"""

class Statement(
	Element,
	collections.Callable,
	collections.Iterable,
):
	"""
	Instances of `Statement` are returned by the `prepare` method of
	`Database` instances.

	A Statement is an Iterable as well as Callable.

	The Iterable interface is supported for queries that take no arguments at
	all. It allows the syntax::

		>>> for x in db.prepare('select * FROM table'):
		...  pass
	"""
	_e_label = 'STATEMENT'
	_e_factors = ('database', 'statement_id', 'string',)

	@property
	@abc.abstractmethod
	def statement_id(self) -> str:
		"""
		The statment's identifier.
		"""

	@property
	@abc.abstractmethod
	def string(self) -> object:
		"""
		The SQL string of the prepared statement.

		`None` if not available. This can happen in cases where a statement is
		prepared on the server and a reference to the statement is sent to the
		client which subsequently uses the statement via the `Database`'s
		`statement` constructor.
		"""

	@property
	@abc.abstractmethod
	def sql_parameter_types(self) -> [str]:
		"""
		The type of the parameters required by the statement.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'VARCHAR', 'INTERVAL']
		"""

	@property
	@abc.abstractmethod
	def sql_column_types(self) -> [str]:
		"""
		The type of the columns produced by the statement.

		A sequence of `str` objects stating the SQL type name::

			['INTEGER', 'VARCHAR', 'INTERVAL']
		"""

	@property
	@abc.abstractmethod
	def pg_parameter_types(self) -> [int]:
		"""
		The type Oids of the parameters required by the statement.

		A sequence of `int` objects stating the PostgreSQL type Oid::

			[27, 28]
		"""

	@property
	@abc.abstractmethod
	def pg_column_types(self) -> [int]:
		"""
		The type Oids of the columns produced by the statement.

		A sequence of `int` objects stating the SQL type name::

			[27, 28]
		"""

	@property
	@abc.abstractmethod
	def column_names(self) -> [str]:
		"""
		The attribute names of the columns produced by the statement.

		A sequence of `str` objects stating the column name::

			['column1', 'column2', 'emp_name']
		"""

	@property
	@abc.abstractmethod
	def column_types(self) -> [type]:
		"""
		The Python types of the columns produced by the statement.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@property
	@abc.abstractmethod
	def parameter_types(self) -> [type]:
		"""
		The Python types expected of parameters given to the statement.

		A sequence of type objects::

			[<class 'int'>, <class 'str'>]
		"""

	@abc.abstractmethod
	def clone(self) -> "Statement":
		"""
		Create a new statement object using the same factors as `self`.

		When used for refreshing plans, the new clone should replace references to
		the original.
		"""

	@abc.abstractmethod
	def close(self) -> None:
		"""
		Close the prepared statement releasing resources associated with it.
		"""
Execution.register(Statement)
PreparedStatement = Statement

class StoredProcedure(
	Element,
	collections.Callable,
):
	"""
	A function stored on the database.
	"""
	_e_label = 'FUNCTION'
	_e_factors = ('database',)

	@abc.abstractmethod
	def __call__(self, *args, **kw) -> (object, Cursor, collections.Iterable):
		"""
		Execute the procedure with the given arguments. If keyword arguments are
		passed they must be mapped to the argument whose name matches the key.
		If any positional arguments are given, they must fill in gaps created by
		the stated keyword arguments. If too few or too many arguments are
		given, a TypeError must be raised. If a keyword argument is passed where
		the procedure does not have a corresponding argument name, then,
		likewise, a TypeError must be raised.

		In the case where the `StoredProcedure` references a set returning
		function(SRF), the result *must* be an iterable. SRFs that return single
		columns *must* return an iterable of that column; not row data. If the
		SRF returns a composite(OUT parameters), it *should* return a `Cursor`.
		"""

##
# Arguably, it would be wiser to isolate blocks, and savepoints, but the utility
# of the separation is not significant. It's really
# more interesting as a formality that the user may explicitly state the
# type of the transaction. However, this capability is not completely absent
# from the current interface as the configuration parameters, or lack thereof,
# help imply the expectations.
class Transaction(Element):
	"""
	A `Tranaction` is an element that represents a transaction in the session.
	Once created, it's ready to be started, and subsequently committed or
	rolled back.

	Read-only transaction:

		>>> with db.xact(mode = 'read only'):
		...  ...

	Read committed isolation:

		>>> with db.xact(isolation = 'READ COMMITTED'):
		...  ...

	Savepoints are created if inside a transaction block:

		>>> with db.xact():
		...  with db.xact():
		...   ...
	"""
	_e_label = 'XACT'
	_e_factors = ('database',)

	@property
	@abc.abstractmethod
	def mode(self) -> (None, str):
		"""
		The mode of the transaction block:

			START TRANSACTION [ISOLATION] <mode>;

		The `mode` property is a string and will be directly interpolated into the
		START TRANSACTION statement.
		"""

	@property
	@abc.abstractmethod
	def isolation(self) -> (None, str):
		"""
		The isolation level of the transaction block:

			START TRANSACTION <isolation> [MODE];

		The `isolation` property is a string and will be directly interpolated into
		the START TRANSACTION statement.
		"""

	@abc.abstractmethod
	def start(self) -> None:
		"""
		Start the transaction.

		If the database is in a transaction block, the transaction should be
		configured as a savepoint. If any transaction block configuration was
		applied to the transaction, raise a `postgresql.exceptions.OperationError`.

		If the database is not in a transaction block, start one using the
		configuration where:

		`self.isolation` specifies the ``ISOLATION LEVEL``. Normally, ``READ
		COMMITTED``, ``SERIALIZABLE``, or ``READ UNCOMMITTED``.

		`self.mode` specifies the mode of the transaction. Normally, ``READ
		ONLY`` or ``READ WRITE``.

		If the transaction is already open, do nothing.

		If the transaction has been committed or aborted, raise an
		`postgresql.exceptions.OperationError`.
		"""
	begin = start

	@abc.abstractmethod
	def commit(self) -> None:
		"""
		Commit the transaction.

		If the transaction is a block, issue a COMMIT statement.

		If the transaction was started inside a transaction block, it should be
		identified as a savepoint, and the savepoint should be released.

		If the transaction has already been committed, do nothing.
		"""

	@abc.abstractmethod
	def rollback(self) -> None:
		"""
		Abort the transaction.

		If the transaction is a savepoint, ROLLBACK TO the savepoint identifier.

		If the transaction is a transaction block, issue an ABORT.

		If the transaction has already been aborted, do nothing.
		"""
	abort = rollback

	@abc.abstractmethod
	def __enter__(self):
		"""
		Run the `start` method and return self.
		"""

	@abc.abstractmethod
	def __exit__(self, typ, obj, tb):
		"""
		If an exception is indicated by the parameters, run the transaction's
		`rollback` method iff the database is still available(not closed), and
		return a `False` value.

		If an exception is not indicated, but the database's transaction state is
		in error, run the transaction's `rollback` method and raise a
		`postgresql.exceptions.InFailedTransactionError`. If the database is
		unavailable, the `rollback` method should cause a
		`postgresql.exceptions.ConnectionDoesNotExistError` exception to occur.

		Otherwise, run the transaction's `commit` method.

		When the `commit` is ultimately unsuccessful or not ran at all, the purpose
		of __exit__ is to resolve the error state of the database iff the
		database is available(not closed) so that more commands can be after the
		block's exit.
		"""

class Settings(
	Element,
	collections.MutableMapping
):
	"""
	A mapping interface to the session's settings. This provides a direct
	interface to ``SHOW`` or ``SET`` commands. Identifiers and values need
	not be quoted specially as the implementation must do that work for the
	user.
	"""
	_e_label = 'SETTINGS'

	@abc.abstractmethod
	def __getitem__(self, key):
		"""
		Return the setting corresponding to the given key. The result should be
		consistent with what the ``SHOW`` command returns. If the key does not
		exist, raise a KeyError.
		"""

	@abc.abstractmethod
	def __setitem__(self, key, value):
		"""
		Set the setting with the given key to the given value. The action should
		be consistent with the effect of the ``SET`` command.
		"""

	@abc.abstractmethod
	def __call__(self, **kw):
		"""
		Create a context manager applying the given settings on __enter__ and
		restoring the old values on __exit__.

		>>> with db.settings(search_path = 'local,public'):
		...  ...
		"""

	@abc.abstractmethod
	def get(self, key, default = None):
		"""
		Get the setting with the corresponding key. If the setting does not
		exist, return the `default`.
		"""

	@abc.abstractmethod
	def getset(self, keys):
		"""
		Return a dictionary containing the key-value pairs of the requested
		settings. If *any* of the keys do not exist, a `KeyError` must be raised
		with the set of keys that did not exist.
		"""

	@abc.abstractmethod
	def update(self, mapping):
		"""
		For each key-value pair, incur the effect of the `__setitem__` method.
		"""

	@abc.abstractmethod
	def keys(self):
		"""
		Return an iterator to all of the settings' keys.
		"""

	@abc.abstractmethod
	def values(self):
		"""
		Return an iterator to all of the settings' values.
		"""

	@abc.abstractmethod
	def items(self):
		"""
		Return an iterator to all of the setting value pairs.
		"""

class Database(Element):
	"""
	The interface to an individual database. `Connection` objects inherit from
	this
	"""
	_e_label = 'DATABASE'

	@property
	@abc.abstractmethod
	def backend_id(self) -> (int, None):
		"""
		The backend's process identifier.
		"""

	@property
	@abc.abstractmethod
	def version_info(self) -> tuple:
		"""
		A version tuple of the database software similar Python's `sys.version_info`.

		>>> db.version_info
		(8, 1, 3, '', 0)
		"""

	@property
	@abc.abstractmethod
	def client_address(self) -> (str, None):
		"""
		The client address that the server sees. This is obtainable by querying
		the ``pg_catalog.pg_stat_activity`` relation.

		`None` if unavailable.
		"""

	@property
	@abc.abstractmethod
	def client_port(self) -> (int, None):
		"""
		The client port that the server sees. This is obtainable by querying
		the ``pg_catalog.pg_stat_activity`` relation.

		`None` if unavailable.
		"""

	@property
	@abc.abstractmethod
	def xact(self,
		isolation : "ISOLATION LEVEL to use with the transaction" = None,
		mode : "Mode of the transaction, READ ONLY or READ WRITE" = None,
	) -> Transaction:
		"""
		Create a `Transaction` object using the given keyword arguments as its
		configuration.
		"""

	@property
	@abc.abstractmethod
	def settings(self) -> Settings:
		"""
		A `Settings` instance bound to the `Database`.
		"""

	@abc.abstractmethod
	def do(language, source) -> None:
		"""
		Execute a DO statement using the given language and source.
		Always returns `None`.

		Likely to be a function of Connection.execute.
		"""

	@abc.abstractmethod
	def execute(sql) -> None:
		"""
		Execute an arbitrary block of SQL. Always returns `None` and raise
		an exception on error.
		"""

	@abc.abstractmethod
	def prepare(self, sql : str) -> Statement:
		"""
		Create a new `Statement` instance bound to the connection
		using the given SQL.

		>>> s = db.prepare("SELECT 1")
		>>> c = s()
		>>> c.next()
		(1,)
		"""

	@abc.abstractmethod
	def statement_from_id(self,
		statement_id : "The statement's identification string.",
	) -> Statement:
		"""
		Create a `Statement` object that was already prepared on the
		server. The distinction between this and a regular query is that it
		must be explicitly closed if it is no longer desired, and it is
		instantiated using the statement identifier as opposed to the SQL
		statement itself.
		"""

	@abc.abstractmethod
	def cursor_from_id(self,
		cursor_id : "The cursor's identification string."
	) -> Cursor:
		"""
		Create a `Cursor` object from the given `cursor_id` that was already
		declared on the server.

		`Cursor` objects created this way must *not* be closed when the object
		is garbage collected. Rather, the user must explicitly close it for
		the server resources to be released. This is in contrast to `Cursor`
		objects that are created by invoking a `Statement` or a SRF
		`StoredProcedure`.
		"""

	@abc.abstractmethod
	def proc(self,
		procedure_id : \
			"The procedure identifier; a valid ``regprocedure`` or Oid."
	) -> StoredProcedure:
		"""
		Create a `StoredProcedure` instance using the given identifier.

		The `proc_id` given can be either an ``Oid``, or a ``regprocedure``
		that identifies the stored procedure to create the interface for.

		>>> p = db.proc('version()')
		>>> p()
		'PostgreSQL 8.3.0'
		>>> qstr = "select oid from pg_proc where proname = 'generate_series'"
		>>> db.prepare(qstr).first()
		1069
		>>> generate_series = db.proc(1069)
		>>> list(generate_series(1,5))
		[1, 2, 3, 4, 5]
		"""

	@abc.abstractmethod
	def reset(self) -> None:
		"""
		Reset the connection into it's original state.

		Issues a ``RESET ALL`` to the database. If the database supports
		removing temporary tables created in the session, then remove them.
		Reapply initial configuration settings such as path.

		The purpose behind this method is to provide a soft-reconnect method
		that re-initializes the connection into its original state. One
		obvious use of this would be in a connection pool where the connection
		is being recycled.
		"""

	@abc.abstractmethod
	def notify(self, *channels, **channel_and_payload) -> int:
		"""
		NOTIFY the channels with the given payload.

		Equivalent to issuing "NOTIFY <channel>" or "NOTIFY <channel>, <payload>"
		for each item in `channels` and `channel_and_payload`. All NOTIFYs issued
		*must* occur in the same transaction.

		The items in `channels` can either be a string or a tuple. If a string,
		no payload is given, but if an item is a `builtins.tuple`, the second item
		will be given as the payload. `channels` offers a means to issue NOTIFYs
		in guaranteed order.

		The items in `channel_and_payload` are all payloaded NOTIFYs where the
		keys are the channels and the values are the payloads. Order is undefined.
		"""

	@abc.abstractmethod
	def listen(self, *channels) -> None:
		"""
		Start listening to the given channels.

		Equivalent to issuing "LISTEN <x>" for x in channels.
		"""

	@abc.abstractmethod
	def unlisten(self, *channels) -> None:
		"""
		Stop listening to the given channels.

		Equivalent to issuing "UNLISTEN <x>" for x in channels.
		"""

	@abc.abstractmethod
	def listening_channels(self) -> ["channel name", ...]:
		"""
		Return an *iterator* to all the channels currently being listened to.
		"""

	@abc.abstractmethod
	def iternotifies(self, timeout = None) -> collections.Iterator:
		"""
		Return an iterator to the notifications received by the connection. The
		iterator *must* produce triples in the form ``(channel, payload, pid)``.

		If timeout is not `None`, `None` *must* be emitted at the specified
		timeout interval. If the timeout is zero, all the pending notifications
		*must* be yielded by the iterator and then `StopIteration` *must* be
		raised.

		If the connection is closed for any reason, the iterator *must* silently
		stop by raising `StopIteration`. Further error control is then the
		responsibility of the user.
		"""

class TypeIO(Element):
	_e_label = 'TYPIO'

	def _e_metas(self):
		return ()

class SocketFactory(object):
	@property
	@abc.abstractmethod
	def fatal_exception(self) -> Exception:
		"""
		The exception that is raised by sockets that indicate a fatal error.

		The exception can be a base exception as the `fatal_error_message` will
		indicate if that particular exception is actually fatal.
		"""

	@property
	@abc.abstractmethod
	def timeout_exception(self) -> Exception:
		"""
		The exception raised by the socket when an operation could not be
		completed due to a configured time constraint.
		"""

	@property
	@abc.abstractmethod
	def tryagain_exception(self) -> Exception:
		"""
		The exception raised by the socket when an operation was interrupted, but
		should be tried again.
		"""

	@property
	@abc.abstractmethod
	def tryagain(self, err : Exception) -> bool:
		"""
		Whether or not `err` suggests the operation should be tried again.
		"""

	@abc.abstractmethod
	def fatal_exception_message(self, err : Exception) -> (str, None):
		"""
		A function returning a string describing the failure, this string will be
		given to the `postgresql.exceptions.ConnectionFailure` instance that will
		subsequently be raised by the `Connection` object.

		Returns `None` when `err` is not actually fatal.
		"""

	@abc.abstractmethod
	def socket_secure(self, socket : "socket object") -> "secured socket":
		"""
		Return a reference to the secured socket using the given parameters.

		If securing the socket for the connector is impossible, the user should
		never be able to instantiate the connector with parameters requesting
		security.
		"""

	@abc.abstractmethod
	def socket_factory_sequence(self) -> [collections.Callable]:
		"""
		Return a sequence of `SocketCreator`s that `Connection` objects will use to
		create the socket object.
		"""

class Category(Element):
	"""
	A category is an object that initializes the subject connection for a
	specific purpose.

	Arguably, a runtime class for use with connections.
	"""
	_e_label = 'CATEGORY'
	_e_factors = ()

	@abc.abstractmethod
	def __call__(self, connection):
		"""
		Initialize the given connection in order to conform to the category.
		"""

class Connector(Element):
	"""
	A connector is an object providing the necessary information to establish a
	connection. This includes credentials, database settings, and many times
	addressing information.
	"""
	_e_label = 'CONNECTOR'
	_e_factors = ('driver', 'category')

	def __call__(self, *args, **kw):
		"""
		Create and connect. Arguments will be given to the `Connection` instance's
		`connect` method.
		"""
		return self.driver.connection(self, *args, **kw)

	def __init__(self,
		user : "required keyword specifying the user name(str)" = None,
		password : str = None,
		database : str = None,
		settings : (dict, [(str,str)]) = None,
		category : Category = None,
	):
		if user is None:
			# sure, it's a "required" keyword, makes for better documentation
			raise TypeError("'user' is a required keyword")
		self.user = user
		self.password = password
		self.database = database
		self.settings = settings
		self.category = category
		if category is not None and not isinstance(category, Category):
			raise TypeError("'category' must a be `None` or `postgresql.api.Category`")

class Connection(Database):
	"""
	The interface to a connection to a PostgreSQL database. This is a
	`Database` interface with the additional connection management tools that
	are particular to using a remote database.
	"""
	_e_label = 'CONNECTION'
	_e_factors = ('connector',)

	@property
	@abc.abstractmethod
	def connector(self) -> Connector:
		"""
		The :py:class:`Connector` instance facilitating the `Connection` object's
		communication and initialization.
		"""

	@property
	@abc.abstractmethod
	def query(self) -> Execution:
		"""
		The :py:class:`Execution` instance providing a one-shot query interface::

			connection.query.<method>(sql, *parameters) == connection.prepare(sql).<method>(*parameters)
		"""

	@property
	@abc.abstractmethod
	def closed(self) -> bool:
		"""
		`True` if the `Connection` is closed, `False` if the `Connection` is
		open.

		>>> db.closed
		True
		"""

	@abc.abstractmethod
	def clone(self) -> "Connection":
		"""
		Create another connection using the same factors as `self`. The returned
		object should be open and ready for use.
		"""

	@abc.abstractmethod
	def connect(self) -> None:
		"""
		Establish the connection to the server and initialize the category.

		Does nothing if the connection is already established.
		"""
		cat = self.connector.category
		if cat is not None:
			cat(self)

	@abc.abstractmethod
	def close(self) -> None:
		"""
		Close the connection.

		Does nothing if the connection is already closed.
		"""

	@abc.abstractmethod
	def __enter__(self):
		"""
		Establish the connection and return self.
		"""

	@abc.abstractmethod
	def __exit__(self, typ, obj, tb):
		"""
		Closes the connection and returns `False` when an exception is passed in,
		`True` when `None`.
		"""

class Driver(Element):
	"""
	The `Driver` element provides the `Connector` and other information
	pertaining to the implementation of the driver. Information about what the
	driver supports is available in instances.
	"""
	_e_label = "DRIVER"
	_e_factors = ()

	@abc.abstractmethod
	def connect(**kw):
		"""
		Create a connection using the given parameters for the Connector.
		"""

class Installation(Element):
	"""
	Interface to a PostgreSQL installation. Instances would provide various
	information about an installation of PostgreSQL accessible by the Python
	"""
	_e_label = "INSTALLATION"
	_e_factors = ()

	@property
	@abc.abstractmethod
	def version(self):
		"""
		A version string consistent with what `SELECT version()` would output.
		"""

	@property
	@abc.abstractmethod
	def version_info(self):
		"""
		A tuple specifying the version in a form similar to Python's
		sys.version_info. (8, 3, 3, 'final', 0)

		See `postgresql.versionstring`.
		"""

	@property
	@abc.abstractmethod
	def type(self):
		"""
		The "type" of PostgreSQL. Normally, the first component of the string
		returned by pg_config.
		"""

	@property
	@abc.abstractmethod
	def ssl(self) -> bool:
		"""
		Whether the installation supports SSL.
		"""

class Cluster(Element):
	"""
	Interface to a PostgreSQL cluster--a data directory. An implementation of
	this provides a means to control a server.
	"""
	_e_label = 'CLUSTER'
	_e_factors = ('installation', 'data_directory')

	@property
	@abc.abstractmethod
	def installation(self) -> Installation:
		"""
		The installation used by the cluster.
		"""

	@property
	@abc.abstractmethod
	def data_directory(self) -> str:
		"""
		The path to the data directory of the cluster.
		"""

	@abc.abstractmethod
	def init(self,
		initdb : "path to the initdb to use" = None,
		user : "name of the cluster's superuser" = None,
		password : "superuser's password" = None,
		encoding : "the encoding to use for the cluster" = None,
		locale : "the locale to use for the cluster" = None,
		collate : "the collation to use for the cluster" = None,
		ctype : "the ctype to use for the cluster" = None,
		monetary : "the monetary to use for the cluster" = None,
		numeric : "the numeric to use for the cluster" = None,
		time : "the time to use for the cluster" = None,
		text_search_config : "default text search configuration" = None,
		xlogdir : "location for the transaction log directory" = None,
	):
		"""
		Create the cluster at the `data_directory` associated with the Cluster
		instance.
		"""

	@abc.abstractmethod
	def drop(self):
		"""
		Kill the server and completely remove the data directory.
		"""

	@abc.abstractmethod
	def start(self):
		"""
		Start the cluster.
		"""

	@abc.abstractmethod
	def stop(self):
		"""
		Signal the server to shutdown.
		"""

	@abc.abstractmethod
	def kill(self):
		"""
		Kill the server.
		"""

	@abc.abstractmethod
	def restart(self):
		"""
		Restart the cluster.
		"""

	@abc.abstractmethod
	def wait_until_started(self,
		timeout : "maximum time to wait" = 10
	):
		"""
		After the start() method is ran, the database may not be ready for use.
		This method provides a mechanism to block until the cluster is ready for
		use.

		If the `timeout` is reached, the method *must* throw a
		`postgresql.exceptions.ClusterTimeoutError`.
		"""

	@abc.abstractmethod
	def wait_until_stopped(self,
		timeout : "maximum time to wait" = 10
	):
		"""
		After the stop() method is ran, the database may still be running.
		This method provides a mechanism to block until the cluster is completely
		shutdown.

		If the `timeout` is reached, the method *must* throw a
		`postgresql.exceptions.ClusterTimeoutError`.
		"""

	@property
	@abc.abstractmethod
	def settings(self):
		"""
		A `Settings` interface to the ``postgresql.conf`` file associated with the
		cluster.
		"""

	@abc.abstractmethod
	def __enter__(self):
		"""
		Start the cluster if it's not already running, and wait for it to be
		readied.
		"""

	@abc.abstractmethod
	def __exit__(self, exc, val, tb):
		"""
		Stop the cluster and wait for it to shutdown *iff* it was started by the
		corresponding enter.
		"""

__docformat__ = 'reStructuredText'
if __name__ == '__main__':
	help(__package__ + '.api')
##
# vim: ts=3:sw=3:noet:
