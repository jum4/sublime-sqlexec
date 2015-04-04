Categories and Libraries
************************

This chapter discusses the usage and implementation of connection categories and
libraries.

.. note::
 First-time users are encouraged to read the `Audience and Motivation`_
 section first.

Libraries are a collection of SQL statements that can be bound to a
connection. Libraries are *normally* bound directly to the connection object as
an attribute using a name specified by the library.

Libraries provide a common way for SQL statements to be managed outside of the
code that uses them. When using ILFs, this increases the portability of the SQL
by keeping the statements isolated from the Python code in an accessible format
that can be easily used by other languages or systems --- An ILF parser can be
implemented within a few dozen lines using basic text tools.

SQL statements defined by a Library are identified by their Symbol. These
symbols are named and annotated in order to allow the user to define how a
statement is to be used. The user may state the default execution method of
the statement object, or whether the symbol is to be preloaded at bind
time--these properties are Symbol Annotations.

The purpose of libraries are to provide a means to manage statements on
disk and at runtime. ILFs provide a means to reference a collection
of statements on disk, and, when loaded, the symbol bindings provides means to
reference a statement already prepared for use on a given connection.

The `postgresql.lib` package-module provides fundamental classes for supporting
categories and libraries.


Writing Libraries
=================

ILF files are the recommended way to build a library. These files use the
naming convention "lib{NAME}.sql". The prefix and suffix are used describe the
purpose of the file and to provide a hint to editors that SQL highlighting
should be used. The format of an ILF takes the form::

	<Preface>
	[name:type:method]
	<statement>
	...

Where multiple symbols may be defined. The Preface that comes before the first
symbol is an arbitrary block of text that should be used to describe the library.
This block is free-form, and should be considered a good place for some
general documentation.

Symbols are named and described using the contents of section markers:
``('[' ... ']')``. Section markers have three components: the symbol name,
the symbol type and the symbol method. Each of these components are separated
using a single colon, ``:``. All components are optional except the Symbol name.
For example::

	[get_user_info]
	SELECT * FROM user WHERE user_id = $1

	[get_user_info_v2::]
	SELECT * FROM user WHERE user_id = $1

In the above example, ``get_user_info`` and ``get_user_info_v2`` are identical.
Empty components indicate the default effect.

The second component in the section identifier is the symbol type. All Symbol
types are listed in `Symbol Types`_. This can be
used to specify what the section's contents are or when to bind the
symbol::

	[get_user_info:preload]
	SELECT * FROM user WHERE user_id = $1

This provides the Binding with the knowledge that the statement should be
prepared when the Library is bound. Therefore, when this Symbol's statement
is used for the first time, it will have already been prepared.

Another type is the ``const`` Symbol type. This defines a data Symbol whose
*statement results* will be resolved when the Library is bound::

	[user_type_ids:const]
	SELECT user_type_id, user_type FROM user_types;

Constant Symbols cannot take parameters as they are data properties. The
*result* of the above query is set to the Bindings' ``user_type_ids``
attribute::

	>>> db.lib.user_type_ids
	<sequence of (user_type_id, user_type)>

Where ``lib`` in the above is a Binding of the Library containing the
``user_type_ids`` Symbol.

Finally, procedures can be bound as symbols using the ``proc`` type::

	[remove_user:proc]
	remove_user(bigint)

All procedures symbols are loaded when the Library is bound. Procedure symbols
are special because the execution method is effectively specified by the
procedure itself.


The third component is the symbol ``method``. This defines the execution method
of the statement and ultimately what is returned when the Symbol is called at
runtime. All the execution methods are listed in `Symbol Execution Methods`_.

The default execution method is the default execution method of
`postgresql.api.PreparedStatement` objects; return the entire result set in a
list object::

	[get_numbers]
	SELECT i FROM generate_series(0, 100-1) AS g(i);

When bound::

	>>> db.lib.get_numbers() == [(x,) for x in range(100)]
	True

The transformation of range in the above is necessary as statements
return a sequence of row objects by default.

For large result-sets, fetching all the rows would be taxing on a system's
memory. The ``rows`` and ``chunks`` methods provide an iterator to rows produced
by a statement using a stream::

	[get_some_rows::rows]
	SELECT i FROM generate_series(0, 1000) AS g(i);

	[get_some_chunks::chunks]
	SELECT i FROM generate_series(0, 1000) AS g(i);

``rows`` means that the Symbol will return an iterator producing individual rows
of the result, and ``chunks`` means that the Symbol will return an iterator
producing sequences of rows of the result.

When bound::

	>>> from itertools import chain
	>>> list(db.lib.get_some_rows()) == list(chain.from_iterable(db.lib.get_some_chunks()))
	True

Other methods include ``column`` and ``first``. The column method provides a
means to designate that the symbol should return an iterator of the values in
the first column instead of an iterator to the rows::

	[another_generate_series_example::column]
	SELECT i FROM generate_series(0, $1::int) AS g(i)

In use::

	>>> list(db.lib.another_generate_series_example(100-1)) == list(range(100))
	True
	>>> list(db.lib.another_generate_series_example(10-1))
	[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

The ``first`` method provides direct access to simple results.
Specifically, the first column of the first row when there is only one column.
When there are multiple columns the first row is returned::

	[get_one::first]
	SELECT 1

	[get_one_twice::first]
	SELECT 1, 1

In use::

	>>> db.lib.get_one() == 1
	True
	>>> db.lib.get_one_twice() == (1,1)
	True

.. note::
 ``first`` should be used with care. When the result returns no rows, `None`
 will be returned.


Using Libraries
===============

After a library is created, it must be loaded before it can be bound using
programmer interfaces. The `postgresql.lib.load` interface provides the
primary entry point for loading libraries.

When ``load`` is given a string, it identifies if a directory separator is in
the string, if there is it will treat the string as a *path* to the ILF to be
loaded. If no separator is found, it will treat the string as the library
name fragment and look for "lib{NAME}.sql" in the directories listed in
`postgresql.sys.libpath`.

Once a `postgresql.lib.Library` instance has been acquired, it can then be
bound to a connection for use. `postgresql.lib.Binding` is used to create an
object that provides and manages the Bound Symbols::

	>>> import postgresql.lib as pg_lib
	>>> lib = pg_lib.load(...)
	>>> B = pg_lib.Binding(db, lib)

The ``B`` object in the above example provides the Library's Symbols as
attributes which can be called to in order to execute the Symbol's statement::

	>>> B.symbol(param)
	...

While it is sometimes necessary, manual creation of a Binding is discouraged.
Rather, `postgresql.lib.Category` objects should be used to manage the set of
Libraries to be bound to a connection.


Categories
----------

Libraries provide access to a collection of symbols; Bindings provide an
interface to the symbols with respect to a subject database. When a connection
is established, multiple Bindings may need to be created in order to fulfill
the requirements of the programmer. When a Binding is created, it exists in
isolation; this can be an inconvenience when access to both the Binding and
the Connection is necessary. Categories exist to provide a formal method for
defining the interface extensions on a `postgresql.api.Database`
instance(connection).

A Category is essentially a runtime-class for connections. It provides a
formal initialization procedure for connection objects at runtime. However,
the connection resource must be connected prior to category initialization.

Categories are sets of Libraries to be bound to a connection with optional name
substitutions. In order to create one directly, pass the Library instances to
`postgresql.lib.Category`::

	>>> import postgresql.lib as pg_lib
	>>> cat = pg_lib.Category(lib1, lib2, libN)

Where ``lib1``, ``lib2``, ``libN`` are `postgresql.lib.Library` instances;
usually created by `postgresql.lib.load`. Once created, categories can then
used by passing the ``category`` keyword to connection creation interfaces::

	>>> import postgresql
	>>> db = postgresql.open(category = cat)

The ``db`` object will now have Bindings for ``lib1``, ``lib2``, ..., and
``libN``.

Categories can alter the access point(attribute name) of Bindings. This is done
by instantiating the Category using keyword parameters::

	>>> cat = pg_lib.Category(lib1, lib2, libname = libN)

At this point, when a connection is established as the category ``cat``,
``libN`` will be bound to the connection object on the attribute ``libname``
instead of the name defined by the library.

And a final illustration of Category usage::

	>>> db = postgresql.open(category = pg_lib.Category(pg_lib.load('name')))
	>>> db.name
	<Library>


Symbol Types
============

The symbol type determines how a symbol is going to be treated by the Binding.
For instance, ``const`` symbols are resolved when the Library is bound and
the statement object is immediately discarded. Here is a list of symbol types
that can be used in ILF libraries:

 ``<default>`` (Empty component)
  The symbol's statement will never change. This allows the Bound Symbol to
  hold onto the `postgresql.api.PreparedStatement` object. When the symbol is
  used again, it will refer to the existing prepared statement object.

 ``preload``
  Like the default type, the Symbol is a simple statement, but it should be
  loaded when the library is bound to the connection.

 ``const``
  The statement takes no parameters and only needs to be executed once. This
  will cause the statement to be executed when the library is bound and the
  results of the statement will be set to the Binding using the symbol name so
  that it may be used as a property by the user.

 ``proc``
  The contents of the section is a procedure identifier. When this type is used
  the symbol method *should not* be specified as the method annotation will be
  automatically resolved based on the procedure's signature.

 ``transient``
  The Symbol is a statement that should *not* be retained. Specifically, it is
  a statement object that will be discarded when the user discard the referenced
  Symbol. Used in cases where the statement is used once or very infrequently.


Symbol Execution Methods
========================

The Symbol Execution Method provides a way to specify how a statement is going
to be used. Specifically, which `postgresql.api.PreparedStatement` method
should be executed when a Bound Symbol is called. The following is a list of
the symbol execution methods and the effect it will have when invoked:

 ``<default>`` (Empty component)
  Returns the entire result set in a single list object. If the statement does
  not return rows, a ``(command, count)`` pair will be returned.

 ``rows``
  Returns an iterator producing each row in the result set.

 ``chunks``
  Returns an iterator producing "chunks" of rows in the result set.

 ``first``
  Returns the first column of the first row if there is one column in the result
  set. If there are multiple columns in the result set, the first row is
  returned. If query is non-RETURNING DML--insert, update, or delete, the row
  count is returned.

 ``column``
  Returns an iterator to values in the first column. (Equivalent to
  executing a statement as ``map(operator.itemgetter(0), ps.rows())``.)

 ``declare``
  Returns a scrollable cursor, `postgresql.api.Cursor`, to the result set.

 ``load_chunks``
  Takes an iterable row-chunks to be given to the statement. Returns `None`. If
  the statement is a ``COPY ... FROM STDIN``, the iterable must produce chunks
  of COPY lines.

 ``load_rows``
  Takes an iterable rows to be given as parameters. If the statement is a ``COPY
  ... FROM STDIN``, the iterable must produce COPY lines.


Reference Symbols
=================

Reference Symbols provide a way to construct a Bound Symbol using the Symbol's
query. When invoked, A Reference Symbol's query is executed in order to produce
an SQL statement to be used as a Bound Symbol. In ILF files, a reference is
identified by its symbol name being prefixed with an ampersand::

	[&refsym::first]
	SELECT 'SELECT 1::int4'::text

Then executed::

	>>> # Runs the 'refsym' SQL, and creates a Bound Symbol using the results.
	>>> sym = lib.refsym()
	>>> assert sym() == 1

The Reference Symbol's type and execution method are inherited by the created
Bound Symbol. With one exception, ``const`` reference symbols are
special in that they immediately resolved into the target Bound Symbol.

A Reference Symbol's source query *must* produce rows of text columns. Multiple
columns and multiple rows may be produced by the query, but they must be
character types as the results are promptly joined together with whitespace so
that the target statement may be prepared.

Reference Symbols are most likely to be used in dynamic DDL and DML situations,
or, somewhat more specifically, any query whose definition depends on a
generated column list.

Distributing and Usage
======================

For applications, distribution and management can easily be a custom
process. The application designates the library directory; the entry point
adds the path to the `postgresql.sys.libpath` list; a category is built; and, a
connection is made using the category.

For mere Python extensions, however, ``distutils`` has a feature that can
aid in ILF distribution. The ``package_data`` setup keyword can be used to
include ILF files alongside the Python modules that make up a project. See
http://docs.python.org/3.1/distutils/setupscript.html#installing-package-data
for more detailed information on the keyword parameter.

The recommended way to manage libraries for extending projects is to
create a package to contain them. For instance, consider the following layout::

	project/
		setup.py
		pkg/
			__init__.py
			lib/
				__init__.py
				libthis.sql
				libthat.sql

The project's SQL libraries are organized into a single package directory,
``lib``, so ``package_data`` would be configured::

	package_data = {'pkg.lib': ['*.sql']}

Subsequently, the ``lib`` package initialization script can then be used to
load the libraries, and create any categories(``project/pkg/lib/__init__.py``)::

	import os.path
	import postgresql.lib as pg_lib
	import postgresql.sys as pg_sys
	libdir = os.path.dirname(__file__)
	pg_sys.libpath.append(libdir)
	libthis = pg_lib.load('this')
	libthat = pg_lib.load('that')
	stdcat = pg_lib.Category(libthis, libthat)

However, it can be undesirable to add the package directory to the global
`postgresql.sys.libpath` search paths. Direct path loading can be used in those
cases::

	import os.path
	import postgresql.lib as pg_lib
	libdir = os.path.dirname(__file__)
	libthis = pg_lib.load(os.path.join(libdir, 'libthis.sql'))
	libthat = pg_lib.load(os.path.join(libdir, 'libthat.sql'))
	stdcat = pg_lib.Category(libthis, libthat)

Using the established project context, a connection would then be created as::

	from pkg.lib import stdcat
	import postgresql as pg
	db = pg.open(..., category = stdcat)
	# And execute some fictitious symbols.
	db.this.sym_from_libthis()
	db.that.sym_from_libthat(...)


Audience and Motivation
=======================

This chapter covers advanced material. It is **not** recommended that categories
and libraries be used for trivial applications or introductory projects.

.. note::
 Libraries and categories are not likely to be of interest to ORM or DB-API users.

With exception to ORMs or other similar abstractions, the most common pattern
for managing connections and statements is delegation::

	class MyAppDB(object):
		def __init__(self, connection):
			self.connection = connection

		def my_operation(self, op_arg1, op_arg2):
			return self.connection.prepare(
				"SELECT my_operation_proc($1,$2)",
			)(op_arg1, op_arg2)
	...

The straightforward nature is likeable, but the usage does not take advantage of
prepared statements. In order to do that an extra condition is necessary to see
if the statement has already been prepared::

	...

	def my_operation(self, op_arg1, op_arg2):
		if self.hasattr(self, '_my_operation'):
			ps = self._my_operation
		else:
			ps = self._my_operation = self.connection.prepare(
				"SELECT my_operation_proc($1, $2)",
			)
		return ps(op_arg1, op_arg2)
	...

There are many variations that can implement the above. It works and it's
simple, but it will be exhausting if repeated and error prone if the
initialization condition is not factored out. Additionally, if access to statement
metadata is needed, the above example is still lacking as it would require
execution of the statement and further protocol expectations to be established.
This is the province of libraries: direct database interface management.

Categories and Libraries are used to factor out and simplify
the above functionality so re-implementation is unnecessary. For example, an
ILF library containing the symbol::

	[my_operation]
	SELECT my_operation_proc($1, $2)

	[<other_symbol>]
	...

Will provide the same functionality as the ``my_operation`` method in the
latter Python implementation.


Terminology
===========

The following terms are used throughout this chapter:

 Annotations
  The information of about a Symbol describing what it is and how it should be
  used.

 Binding
  An interface to the Symbols provided by a Library for use with a given
  connection.

 Bound Symbol
  An interface to an individual Symbol ready for execution against the subject
  database.

 Bound Reference
  An interface to an individual Reference Symbol that will produce a Bound
  Symbol when executed.

 ILF
  INI-style Library Format. "lib{NAME}.sql" files.

 Library
  A collection of Symbols--mapping of names to SQL statements.

 Local Symbol
  A relative term used to denote a symbol that exists in the same library as
  the subject symbol.

 Preface
  The block of text that comes before the first symbol in an ILF file.

 Symbol
  An named database operation provided by a Library. Usually, an SQL statement
  with Annotations.

 Reference Symbol
  A Symbol whose SQL statement *produces* the source for a Bound Symbol.

 Category
  An object supporting a classification for connectors that provides database
  initialization facilities for produced connections. For libraries,
  `postgresql.lib.Category` objects are a set of Libraries,
  `postgresql.lib.Library`.
