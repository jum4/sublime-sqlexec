Changes in v1.1
===============

1.1.0
-----

 * Remove two-phase commit interfaces per deprecation in v1.0.
   For proper two phase commit use, a lock manager must be employed that
   the implementation did nothing to accommodate for.
 * Add support for unpacking anonymous records (Elvis)
 * Support PostgreSQL 9.2 (Elvis)
 * Python 3.3 Support (Elvis)
 * Add column execution method. (jwp)
 * Add one-shot statement interface. Connection.query.* (jwp)
 * Modify the inet/cidr support by relying on the ipaddress module introduced in Python 3.3 (Google's ipaddr project)
   The existing implementation relied on simple str() representation supported by the
   socket module. Unfortunately, MS Windows' socket library does not appear to support the
   necessary functionality, or Python's socket module does not expose it. ipaddress fixes
   the problem.

.. note::
 The `ipaddress` module is now required for local inet and cidr. While it is
 of "preliminary" status, the ipaddr project has been around for some time and
 well supported. ipaddress appears to be the safest way forward for native
 network types.
