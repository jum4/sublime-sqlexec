Administration
==============

This chapter covers the administration of py-postgresql. This includes
installation and other aspects of working with py-postgresql such as
environment variables and configuration files.

Installation
------------

py-postgresql uses Python's distutils package to manage the build and
installation process of the package. The normal entry point for
this is the ``setup.py`` script contained in the root project directory.

After extracting the archive and changing the into the project's directory,
installation is normally as simple as::

	$ python3 ./setup.py install

However, if you need to install for use with a particular version of python,
just use the path of the executable that should be used::

	$ /usr/opt/bin/python3 ./setup.py install


Environment
-----------

These environment variables effect the operation of the package:

 ============== ===============================================================================
 PGINSTALLATION The path to the ``pg_config`` executable of the installation to use by default.
 ============== ===============================================================================
