.. _notifyman:

***********************
Notification Management
***********************

Relevant SQL commands: `NOTIFY <http://postgresql.org/docs/current/static/sql-notify.html>`_,
`LISTEN <http://postgresql.org/docs/current/static/sql-listen.html>`_,
`UNLISTEN <http://postgresql.org/docs/current/static/sql-unlisten.html>`_.

Asynchronous notifications offer a means for PostgreSQL to signal application
code. Often these notifications are used to signal cache invalidation. In 9.0
and greater, notifications may include a "payload" in which arbitrary data may
be delivered on a channel being listened to.

By default, received notifications will merely be appended to an internal
list on the connection object. This list will remain empty for the duration
of a connection *unless* the connection begins listening to a channel that
receives notifications.

The `postgresql.notifyman.NotificationManager` class is used to wait for
messages to come in on a set of connections, pick up the messages, and deliver
the messages to the object's user via the `collections.Iterator` protocol.


Listening on a Single Connection
================================

The ``db.iternotifies()`` method is a simplification of the notification manager. It
returns an iterator to the notifications received on the subject connection.
The iterator yields triples consisting of the ``channel`` being
notified, the ``payload`` sent with the notification, and the ``pid`` of the
backend that caused the notification::

	>>> db.listen('for_rabbits')
	>>> db.notify('for_rabbits')
	>>> for x in db.iternotifies():
	...  channel, payload, pid = x
	...  break
	>>> assert channel == 'for_rabbits'
	True
	>>> assert payload == ''
	True
	>>> assert pid == db.backend_id
	True

The iterator, by default, will continue listening forever unless the connection
is terminated--thus the immediate ``break`` statement in the above loop. In
cases where some additional activity is necessary, a timeout parameter may be
given to the ``iternotifies`` method in order to allow "idle" events to occur
at the designated frequency::

	>>> for x in db.iternotifies(0.5):
	...  if x is None:
	...   break

The above example illustrates that idle events are represented using `None`
objects. Idle events are guaranteed to occur *approximately* at the
specified interval--the ``timeout`` keyword parameter. In addition to
providing a means to do other processing or polling, they also offer a safe
break point for the loop. Internally, the iterator produced by the
``iternotifies`` method *is* a `NotificationManager`, which will localize the
notifications prior to emitting them via the iterator.
*It's not safe to break out of the loop, unless an idle event is being handled.*
If the loop is broken while a regular event is being processed, some events may
remain in the iterator. In order to consume those events, the iterator *must*
be accessible.

The iterator will be exhausted when the connection is closed, but if the
connection is closed during the loop, any remaining notifications *will*
be emitted prior to the loop ending, so it is important to be prepared to
handle exceptions or check for a closed connection.

In situations where multiple connections need to be watched, direct use of the
`NotificationManager` is necessary.


Listening on Multiple Connections
=================================

The `postgresql.notifyman.NotificationManager` class is used to manage
*connections* that are expecting to receive notifications. Instances are
iterators that yield the connection object and notifications received on the
connection or `None` in the case of an idle event. The manager emits events as
a pair; the connection object that received notifications, and *all* the
notifications picked up on that connection::

	>>> from postgresql.notifyman import NotificationManager
	>>> # Using ``nm`` to reference the manager from here on.
	>>> nm = NotificationManager(db1, db2, ..., dbN)
	>>> nm.settimeout(2)
	>>> for x in nm:
	...  if x is None:
	...   # idle
	...   break
	...  
	...  db, notifies = x
	...  for channel, payload, pid in notifies:
	...   ...

The manager will continue to wait for and emit events so long as there are
good connections available in the set; it is possible for connections to be
added and removed at any time. Although, in rare circumstances, discarded
connections may still have pending events if it not removed during an idle
event. The ``connections`` attribute on `NotificationManager` objects is a
set object that may be used directly in order to add and remove connections
from the manager::

	>>> y = []
	>>> for x in nm:
	...  if x is None:
	...   if y:
	...    nm.connections.add(y[0])
	...    del y[0]
	...  

The notification manager is resilient; if a connection dies, it will discard the
connection from the set, and add it to the set of bad connections, the 
``garbage`` attribute. In these cases, the idle event *should* be leveraged to
check for these failures if that's a concern. It is the user's
responsibility to explicitly handle the failure cases, and remove the bad
connections from the ``garbage`` set::

	>>> for x in nm:
	...  if x is None:
	...   if nm.garbage:
	...    recovered = take_out_trash(nm.garbage)
	...    nm.connections.update(recovered)
	...    nm.garbage.clear()
	...  db, notifies = x
	...  for channel, payload, pid in notifies:
	...   ...

Explicitly removing connections from the set can also be a means to gracefully
terminate the event loop::

	>>> for x in nm:
	...  if x in None:
	...   if done_listening is True:
	...    nm.connections.clear()

However, doing so inside the loop is not a requirement; it is safe to remove a
connection from the set at any point.


Notification Managers
=====================

The `postgresql.notifyman.NotificationManager` is an event loop that services
multiple connections. In cases where only one connection needs to be serviced,
the `postgresql.api.Database.iternotifies` method can be used to simplify the
process.


Notification Manager Constructors
---------------------------------

 ``NotificationManager(*connections, timeout = None)``
  Create a NotificationManager instance that manages the notifications coming
  from the given set of connections. The ``timeout`` keyword is optional and
  can be configured using the ``settimeout`` method as well.


Notification Manager Interface Points
-------------------------------------

 ``NotificationManager.__iter__()``
  Returns the instance; it is an iterator.

 ``NotificationManager.__next__()``
  Normally, yield the pair, connection and notifications list, when the next
  event is received. If a timeout is configured, `None` may be yielded to signal
  an idle event. The notifications list is a list of triples:
  ``(channel, payload, pid)``.

 ``NotificationManager.settimeout(timeout : int)``
  Set the amount of time to wait before the manager yields an idle event.
  If zero, the manager will never wait and only yield notifications that are
  immediately available.
  If `None`, the manager will never emit idle events.

 ``NotificationManager.gettimeout() -> [int, None]``
  Get the configured timeout; returns either `None`, or an `int`.

 ``NotificationManager.connections``
  The set of connections that the manager is actively watching for
  notifications. Connections may be added or removed from the set at any time.

 ``NotificationManager.garbage``
  The set of connections that failed. Normally empty, but when a connection gets
  an exceptional condition or explicitly raises an exception, it is removed from
  the ``connections`` set, and placed in ``garbage``.


Zero Timeout
------------

When a timeout of zero, ``0``, is configured, the notification manager will
terminate early. Specifically, each connection will be polled for any pending
notifications, and once all of the collected notifications have been emitted
by the iterator, `StopIteration` will be raised. Notably, *no* idle events will
occur when the timeout is configured to zero.

Zero timeouts offer a means for the notification "queue" to be polled. Often,
this is the appropriate way to collect pending notifications on active
connections where using the connection exclusively for waiting is not
practical::

	>>> notifies = list(db.iternotifies(0))

Or with a NotificationManager instance::

	>>> nm.settimeout(0)
	>>> db_notifies = list(nm)

In both cases of zero timeout, the iterator may be promptly discarded without
losing any events.


Summary of Characteristics
--------------------------

 * The iterator will continue until the connections die.
 * Objects yielded by the iterator are either `None`, an "idle event", or an
   individual notification triple if using ``db.iternotifies()``, or a
   ``(db, notifies)`` pair if using the base `NotificationManager`.
 * When a connection dies or raises an exception, it will be removed from
   the ``nm.connections`` set and added to the ``nm.garbage`` set.
 * The NotificationManager instance will *not* hold any notifications
   during an idle event. Idle events offer a break point in which the manager
   may be discarded.
 * A timeout of zero will cause the iterator to only yield the events
   that are pending right now, and promptly end. However, the same manager
   object may be used again.
 * A notification triple is a tuple consisting of ``(channel, payload, pid)``.
 * Connections may be added and removed from the ``nm.connections`` set at
   any time.
