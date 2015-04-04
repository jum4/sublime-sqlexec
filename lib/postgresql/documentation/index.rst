py-postgresql
=============

py-postgresql is a project dedicated to improving the Python client interfaces to PostgreSQL.

At its core, py-postgresql provides a PG-API, `postgresql.api`, and
DB-API 2.0 interface for using a PostgreSQL database.

Contents
--------

.. toctree::
   :maxdepth: 2

   admin
   driver
   copyman
   notifyman
   alock
   cluster
   lib
   clientparameters
   gotchas

Reference
---------

.. toctree::
   :maxdepth: 2

   bin
   reference

Changes
-------

.. toctree::
   :maxdepth: 1

   changes-v1.1
   changes-v1.0

Sample Code
-----------

Using `postgresql.driver`::

   >>> import postgresql
   >>> db = postgresql.open("pq://user:password@host/name_of_database")
   >>> db.execute("CREATE TABLE emp (emp_name text PRIMARY KEY, emp_salary numeric)")
   >>>
   >>> # Create the statements.
   >>> make_emp = db.prepare("INSERT INTO emp VALUES ($1, $2)")
   >>> raise_emp = db.prepare("UPDATE emp SET emp_salary = emp_salary + $2 WHERE emp_name = $1")
   >>> get_emp_with_salary_lt = db.prepare("SELECT emp_name FROM emp WHERE emp_salay < $1")
   >>>
   >>> # Create some employees, but do it in a transaction--all or nothing.
   >>> with db.xact():
   ...  make_emp("John Doe", "150,000")
   ...  make_emp("Jane Doe", "150,000")
   ...  make_emp("Andrew Doe", "55,000")
   ...  make_emp("Susan Doe", "60,000")
   >>>
   >>> # Give some raises
   >>> with db.xact():
   ...  for row in get_emp_with_salary_lt("125,000"):
   ...   print(row["emp_name"])
   ...   raise_emp(row["emp_name"], "10,000")

Of course, if DB-API 2.0 is desired, the module is located at
`postgresql.driver.dbapi20`. DB-API extends PG-API, so the features
illustrated above are available on DB-API connections.

See :ref:`db_interface` for more information.
