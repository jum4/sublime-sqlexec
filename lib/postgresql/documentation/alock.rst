.. _alock:

**************
Advisory Locks
**************

.. warning:: `postgresql.alock` is a new feature in v1.0.

`Explicit Locking in PostgreSQL <http://www.postgresql.org/docs/current/static/explicit-locking.html#ADVISORY-LOCKS>`_.

PostgreSQL's advisory locks offer a cooperative synchronization primitive.
These are used in cases where an application needs access to a resource, but
using table locks may cause interference with other operations that can be
safely performed alongside the application-level, exclusive operation.

Advisory locks can be used by directly executing the stored procedures in the
database or by using the :class:`postgresql.alock.ALock` subclasses, which
provides a context manager that uses those stored procedures.

Currently, only two subclasses exist. Each represents the lock mode
supported by PostgreSQL's advisory locks:

 * :class:`postgresql.alock.ShareLock`
 * :class:`postgresql.alock.ExclusiveLock`


Acquiring ALocks
================

An ALock instance represents a sequence of advisory locks. A single ALock can
acquire and release multiple advisory locks by creating the instance with
multiple lock identifiers::

	>>> from postgresql import alock
	>>> table1_oid = 192842
	>>> table2_oid = 192849
	>>> l = alock.ExclusiveLock(db, (table1_oid, 0), (table2_oid, 0))
	>>> l.acquire()
	>>> ...
	>>> l.release()

:class:`postgresql.alock.ALock` is similar to :class:`threading.RLock`; in
order for an ALock to be released, it must be released the number of times it
has been acquired. ALocks are associated with and survived by their session.
Much like how RLocks are associated with the thread they are acquired in:
acquiring an ALock again will merely increment its count.

PostgreSQL allows advisory locks to be identified using a pair of `int4` or a
single `int8`. ALock instances represent a *sequence* of those identifiers::

	>>> from postgresql import alock
	>>> ids = [(0,0), 0, 1]
	>>> with alock.ShareLock(db, *ids):
	...  ...

Both types of identifiers may be used within the same ALock, and, regardless of
their type, will be aquired in the order that they were given to the class'
constructor. In the above example, ``(0,0)`` is acquired first, then ``0``, and
lastly ``1``.


ALocks
======

`postgresql.alock.ALock` is abstract; it defines the interface and some common
functionality. The lock mode is selected by choosing the appropriate subclass.

There are two:

 ``postgresql.alock.ExclusiveLock(database, *identifiers)``
  Instantiate an ALock object representing the `identifiers` for use with the
  `database`. Exclusive locks will conflict with other exclusive locks and share
  locks.

 ``postgresql.alock.ShareLock(database, *identifiers)``
  Instantiate an ALock object representing the `identifiers` for use with the
  `database`. Share locks can be acquired when a share lock with the same
  identifier has been acquired by another backend. However, an exclusive lock
  with the same identifier will conflict.


ALock Interface Points
----------------------

Methods and properties available on :class:`postgresql.alock.ALock` instances:

 ``alock.acquire(blocking = True)``
  Acquire the advisory locks represented by the ``alock`` object. If blocking is
  `True`, the default, the method will block until locks on *all* the
  identifiers have been acquired.

  If blocking is `False`, acquisition may not block, and success will be
  indicated by the returned object: `True` if *all* lock identifiers were
  acquired and `False` if any of the lock identifiers could not be acquired.

 ``alock.release()``
  Release the advisory locks represented by the ``alock`` object. If the lock
  has not been acquired, a `RuntimeError` will be raised.

 ``alock.locked()``
  Returns a boolean describing whether the locks are held or not. This will
  return `False` if the lock connection has been closed.

 ``alock.__enter__()``
  Alias to ``acquire``; context manager protocol. Always blocking.

 ``alock.__exit__(typ, val, tb)``
  Alias to ``release``; context manager protocol.
