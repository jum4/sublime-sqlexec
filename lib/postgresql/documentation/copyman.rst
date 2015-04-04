.. _pg_copyman:

***************
Copy Management
***************

The `postgresql.copyman` module provides a way to quickly move COPY data coming
from one connection to many connections. Alternatively, it can be sourced
by arbitrary iterators and target arbitrary callables.

Statement execution methods offer a way for running COPY operations
with iterators, but the cost of allocating objects for each row is too
significant for transferring gigabytes of COPY data from one connection to
another. The interfaces available on statement objects are primarily intended to
be used when transferring COPY data to and from arbitrary Python
objects.

Direct connection-to-connection COPY operations can be performed using the
high-level `postgresql.copyman.transfer` function::

	>>> from postgresql import copyman
	>>> send_stmt = source.prepare("COPY (SELECT i FROM generate_series(1, 1000000) AS g(i)) TO STDOUT")
	>>> destination.execute("CREATE TEMP TABLE loading_table (i int8)")
	>>> receive_stmt = destination.prepare("COPY loading_table FROM STDIN")
	>>> total_rows, total_bytes = copyman.transfer(send_stmt, receive_stmt)

However, if more control is needed, the `postgresql.copyman.CopyManager` class
should be used directly.


Copy Managers
=============

The `postgresql.copyman.CopyManager` class manages the Producer and the
Receivers involved in a COPY operation. Normally,
`postgresql.copyman.StatementProducer` and
`postgresql.copyman.StatementReceiver` instances. Naturally, a Producer is the
object that produces the COPY data to be given to the Manager's Receivers.

Using a Manager directly means that there is a need for more control over
the operation. The Manager is both a context manager and an iterator. The
context manager interfaces handle initialization and finalization of the COPY
state, and the iterator provides an event loop emitting information about the
amount of COPY data transferred this cycle. Normal usage takes the form::

	>>> from postgresql import copyman
	>>> send_stmt = source.prepare("COPY (SELECT i FROM generate_series(1, 1000000) AS g(i)) TO STDOUT")
	>>> destination.execute("CREATE TEMP TABLE loading_table (i int8)")
	>>> receive_stmt = destination.prepare("COPY loading_table FROM STDIN")
	>>> producer = copyman.StatementProducer(send_stmt)
	>>> receiver = copyman.StatementReceiver(receive_stmt)
	>>> 
	>>> with source.xact(), destination.xact():
	...  with copyman.CopyManager(producer, receiver) as copy:
	...   for num_messages, num_bytes in copy:
	...    update_rate(num_bytes)

As an alternative to a for-loop inside a with-statement block, the `run` method
can be called to perform the operation::

	>>> with source.xact(), destination.xact():
	...  copyman.CopyManager(producer, receiver).run()

However, there is little benefit beyond using the high-level
`postgresql.copyman.transfer` function.

Manager Interface Points
------------------------

Primarily, the `postgresql.copyman.CopyManager` provides a context manager and
an iterator for controlling the COPY operation.

 ``CopyManager.run()``
  Perform the entire COPY operation.

 ``CopyManager.__enter__()``
  Start the COPY operation. Connections taking part in the COPY should **not**
  be used until ``__exit__`` is ran.

 ``CopyManager.__exit__(typ, val, tb)``
  Finish the COPY operation. Fails in the case of an incomplete
  COPY, or an untrapped exception. Either returns `None` or raises the generalized
  exception, `postgresql.copyman.CopyFail`.

 ``CopyManager.__iter__()``
  Returns the CopyManager instance.

 ``CopyManager.__next__()``
  Transfer the next chunk of COPY data to the receivers. Yields a tuple
  consisting of the number of messages and bytes transferred,
  ``(num_messages, num_bytes)``. Raises `StopIteration` when complete.

  Raises `postgresql.copyman.ReceiverFault` when a Receiver raises an
  exception.
  Raises `postgresql.copyman.ProducerFault` when the Producer raises an
  exception. The original exception is available via the exception's
  ``__context__`` attribute.

 ``CopyManager.reconcile(faulted_receiver)``
  Reconcile a faulted receiver. When a receiver faults, it will no longer
  be in the set of Receivers. This method is used to signal to the manager that the
  problem has been corrected, and the receiver is again ready to receive.

 ``CopyManager.receivers``
  The `builtins.set` of Receivers involved in the COPY operation.

 ``CopyManager.producer``
  The Producer emitting the data to be given to the Receivers.


Faults
======

The CopyManager generalizes any exceptions that occur during transfer. While
inside the context manager, `postgresql.copyman.Fault` may be raised if a
Receiver or a Producer raises an exception. A `postgresql.copyman.ProducerFault`
in the case of the Producer, and `postgresql.copyman.ReceiverFault` in the case
of the Receivers.

.. note::
 Faults are only raised by `postgresql.copyman.CopyManager.__next__`. The
 ``run()`` method will only raise `postgresql.copyman.CopyFail`.

Receiver Faults
---------------

The Manager assumes the Fault is fatal to a Receiver, and immediately removes
it from the set of target receivers. Additionally, if the Fault exception goes
untrapped, the copy will ultimately fail.

The Fault exception references the Manager that raised the exception, and the
actual exceptions that occurred associated with the Receiver that caused them.

In order to identify the exception that caused a Fault, the ``faults`` attribute
on the `postgresql.copyman.ReceiverFault` must be referenced::

	>>> from postgresql import copyman
	>>> send_stmt = source.prepare("COPY (SELECT i FROM generate_series(1, 1000000) AS g(i)) TO STDOUT")
	>>> destination.execute("CREATE TEMP TABLE loading_table (i int8)")
	>>> receive_stmt = destination.prepare("COPY loading_table FROM STDIN")
	>>> producer = copyman.StatementProducer(send_stmt)
	>>> receiver = copyman.StatementReceiver(receive_stmt)
	>>> 
	>>> with source.xact(), destination.xact():
	...  with copyman.CopyManager(producer, receiver) as copy:
	...   while copy.receivers:
	...    try:
	...     for num_messages, num_bytes in copy:
	...      update_rate(num_bytes)
	...     break
	...    except copyman.ReceiverFault as cf:
	...     # Access the original exception using the receiver as the key.
	...     original_exception = cf.faults[receiver]
	...     if unknown_failure(original_exception):
	...      ...
	...      raise


ReceiverFault Properties
~~~~~~~~~~~~~~~~~~~~~~~~

The following attributes exist on `postgresql.copyman.ReceiverFault` instances:

 ``ReceiverFault.manager``
  The subject `postgresql.copyman.CopyManager` instance.

 ``ReceiverFault.faults``
  A dictionary mapping the Receiver to the exception raised by that Receiver.


Reconciliation
~~~~~~~~~~~~~~

When a `postgresql.copyman.ReceiverFault` is raised, the Manager immediately
removes the Receiver so that the COPY operation can continue. Continuation of
the COPY can occur by trapping the exception and continuing the iteration of the
Manager. However, if the fault is recoverable, the
`postgresql.copyman.CopyManager.reconcile` method must be used to reintroduce the
Receiver into the Manager's set. Faults must be trapped from within the
Manager's context::

	>>> import socket
	>>> from postgresql import copyman
	>>> send_stmt = source.prepare("COPY (SELECT i FROM generate_series(1, 1000000) AS g(i)) TO STDOUT")
	>>> destination.execute("CREATE TEMP TABLE loading_table (i int8)")
	>>> receive_stmt = destination.prepare("COPY loading_table FROM STDIN")
	>>> producer = copyman.StatementProducer(send_stmt)
	>>> receiver = copyman.StatementReceiver(receive_stmt)
	>>> 
	>>> with source.xact(), destination.xact():
	...  with copyman.CopyManager(producer, receiver) as copy:
	...   while copy.receivers:
	...    try:
	...     for num_messages, num_bytes in copy:
	...      update_rate(num_bytes)
	...    except copyman.ReceiverFault as cf:
	...     if isinstance(cf.faults[receiver], socket.timeout):
	...      copy.reconcile(receiver)
	...     else:
	...      raise

Recovering from Faults does add significant complexity to a COPY operation,
so, often, it's best to avoid conditions in which reconciliable Faults may
occur.


Producer Faults
---------------

Producer faults are normally fatal to the COPY operation and should rarely be
trapped. However, the Manager makes no state changes when a Producer faults,
so, unlike Receiver Faults, no reconciliation process is necessary; rather,
if it's safe to continue, the Manager's iterator should continue to be
processed.

ProducerFault Properties
~~~~~~~~~~~~~~~~~~~~~~~~

The following attributes exist on `postgresql.copyman.ProducerFault` instances:

 ``ReceiverFault.manager``
  The subject `postgresql.copyman.CopyManager`.

 ``ReceiverFault.__context__``
  The original exception raised by the Producer.


Failures
========

When a COPY operation is aborted, either by an exception or by the iterator
being broken, a `postgresql.copyman.CopyFail` exception will be raised by the
Manager's ``__exit__()`` method. The `postgresql.copyman.CopyFail` exception
offers to record any exceptions that occur during the exit of the context
managers of the Producer and the Receivers.


CopyFail Properties
-------------------

The following properties exist on `postgresql.copyman.CopyFail` exceptions:

 ``CopyFail.manager``
  The Manager whose COPY operation failed.

 ``CopyFail.receiver_faults``
  A dictionary mapping a `postgresql.copyman.Receiver` to the exception raised
  by that Receiver's ``__exit__``. `None` if no exceptions were raised by the
  Receivers.

 ``CopyFail.producer_fault``
  The exception Raised by the `postgresql.copyman.Producer`. `None` if none.


Producers
=========

The following Producers are available:

 ``postgresql.copyman.StatementProducer(postgresql.api.Statement)``
  Given a Statement producing COPY data, construct a Producer.

 ``postgresql.copyman.IteratorProducer(collections.Iterator)``
  Given an Iterator producing *chunks* of COPY lines, construct a Producer to
  manage the data coming from the iterator.


Receivers
=========

 ``postgresql.copyman.StatementReceiver(postgresql.api.Statement)``
  Given a Statement producing COPY data, construct a Producer.

 ``postgresql.copyman.CallReceiver(callable)``
  Given a callable, construct a Receiver that will transmit COPY data in chunks
  of lines. That is, the callable will be given a list of COPY lines for each
  transfer cycle.


Terminology
===========

The following terms are regularly used to describe the implementation and
processes of the `postgresql.copyman` module:

 Manager
  The object used to manage data coming from a Producer and being given to the
  Receivers. It also manages the necessary initialization and finalization steps
  required by those factors.

 Producer
  The object used to produce the COPY data to be given to the Receivers. The
  source.

 Receiver
  An object that consumes COPY data. A target.

 Fault
  Specifically, `postgresql.copyman.Fault` exceptions. A Fault is raised
  when a Receiver or a Producer raises an exception during the COPY operation.

 Reconciliation
  Generally, the steps performed by the "reconcile" method on
  `postgresql.copyman.CopyManager` instances. More precisely, the
  necessary steps for a Receiver's reintroduction into the COPY operation after
  a Fault.

 Failed Copy
  A failed copy is an aborted COPY operation. This occurs in situations of
  untrapped exceptions or an incomplete COPY. Specifically, the COPY will be
  noted as failed in cases where the Manager's iterator is *not* ran until
  exhaustion.

 Realignment
  The process of providing compensating data to the Receivers so that the
  connection will be on a message boundary. Occurs when the COPY operation
  is aborted.
