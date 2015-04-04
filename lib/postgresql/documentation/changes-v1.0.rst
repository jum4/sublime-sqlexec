Changes in v1.0
===============

1.0.4 in development
--------------------

 * Alter how changes are represented in documentation to simplify merging.

1.0.3 released on 2011-09-24
----------------------------

 * Use raise x from y to generalize exceptions. (Elvis Pranskevichus)
 * Alter postgresql.string.quote_ident to always quote. (Elvis Pranskevichus)
 * Add postgresql.string.quote_ident_if_necessary (Modification of Elvis Pranskevichus' patch)
 * Many postgresql.string bug fixes (Elvis Pranskevichus)
 * Correct ResourceWarnings improving Python 3.2 support. (jwp)
 * Add test command to setup.py (Elvis Pranskevichus)

1.0.2 released on 2010-09-18
----------------------------

 * Add support for DOMAINs in registered composites. (Elvis Pranskevichus)
 * Properly raise StopIteration in Cursor.__next__. (Elvis Pranskevichus)
 * Add Cluster Management documentation.
 * Release savepoints after rolling them back.
 * Fix Startup() usage for Python 3.2.
 * Emit deprecation warning when 'gid' is given to xact().
 * Compensate for Python3.2's ElementTree API changes.

1.0.1 released on 2010-04-24
----------------------------

 * Fix unpacking of array NULLs. (Elvis Pranskevichus)
 * Fix .first()'s handling of counts and commands.
   Bad logic caused zero-counts to return the command tag.
 * Don't interrupt and close a temporal connection if it's not open.
 * Use the Driver's typio attribute for TypeIO overrides. (Elvis Pranskevichus)

1.0 released on 2010-03-27
--------------------------

 * **DEPRECATION**: Removed 2PC support documentation.
 * **DEPRECATION**: Removed pg_python and pg_dotconf 'scripts'.
   They are still accessible by python3 -m postgresql.bin.pg_*
 * Add support for binary hstore.
 * Add support for user service files.
 * Implement a Copy manager for direct connection-to-connection COPY operations.
 * Added db.do() method for DO-statement support(convenience method).
 * Set the default client_min_messages level to WARNING.
   NOTICEs are often not desired by programmers, and py-postgresql's
   high verbosity further irritates that case.
 * Added postgresql.project module to provide project information.
   Project name, author, version, etc.
 * Increased default recvsize and chunksize for improved performance.
 * 'D' messages are special cased as builtins.tuples instead of
   protocol.element3.Tuple
 * Alter Statement.chunks() to return chunks of builtins.tuple. Being
   an interface intended for speed, types.Row() impedes its performance.
 * Fix handling of infinity values with timestamptz, timestamp, and date.
   [Bug reported by Axel Rau.]
 * Correct representation of PostgreSQL ARRAYs by properly recording
   lowerbounds and upperbounds. Internally, sub-ARRAYs have their own
   element lists.
 * Implement a NotificationManager for managing the NOTIFYs received
   by a connection. The class can manage NOTIFYs from multiple
   connections, whereas the db.wait() method is tailored for single targets.
 * Implement an ALock class for managing advisory locks using the
   threading.Lock APIs. [Feedback from Valentine Gogichashvili]
 * Implement reference symbols. Allow libraries to define symbols that
   are used to create queries that inherit the original symbol's type and
   execution method. ``db.prepare(db.prepare(...).first())``
 * Fix handling of unix domain sockets by pg.open and driver.connect.
   [Reported by twitter.com/rintavarustus]
 * Fix typo/dropped parts of a raise LoadError in .lib.
   [Reported by Vlad Pranskevichus]
 * Fix db.tracer and pg_python's --pq-trace=
 * Fix count return from .first() method. Failed to provide an empty
   tuple for the rformats of the bind statement.
   [Reported by dou dou]
