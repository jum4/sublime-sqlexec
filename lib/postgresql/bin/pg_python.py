##
# .bin.pg_python - Python console with a connection.
##
"""
Python command with a PG-API connection(``db``).
"""
import os
import sys
import re
import code
import optparse
import contextlib
from .. import clientparameters
from ..python import command as pycmd
from .. import project

from ..driver import default as pg_driver
from .. import exceptions as pg_exc
from .. import sys as pg_sys
from .. import lib as pg_lib

pq_trace = optparse.make_option(
	'--pq-trace',
	dest = 'pq_trace',
	help = 'trace PQ protocol transmissions',
	default = None,
)
default_options = [
	pq_trace,
	clientparameters.option_lib,
	clientparameters.option_libpath,
] + pycmd.default_optparse_options

def command(argv = sys.argv):
	p = clientparameters.DefaultParser(
		"%prog [connection options] [script] ...",
		version = project.version,
		option_list = default_options
	)
	p.disable_interspersed_args()
	co, ca = p.parse_args(argv[1:])
	rv = 1

	# Resolve the category.
	pg_sys.libpath.insert(0, os.path.curdir)
	pg_sys.libpath.extend(co.libpath or [])
	if co.lib:
		cat = pg_lib.Category(*map(pg_lib.load, co.lib))
	else:
		cat = None

	trace_file = None
	if co.pq_trace is not None:
		trace_file = open(co.pq_trace, 'a')

	try:
		need_prompt = False
		cond = None
		connector = None
		connection = None
		while connection is None:
			try:
				cond = clientparameters.collect(parsed_options = co, prompt_title = None)
				if need_prompt:
					# authspec error thrown last time, so force prompt.
					cond['prompt_password'] = True
				try:
					clientparameters.resolve_password(cond, prompt_title = 'pg_python')
				except EOFError:
					raise SystemExit(1)
				connector = pg_driver.fit(category = cat, **cond)
				connection = connector()
				if trace_file is not None:
					connection.tracer = trace_file.write
				connection.connect()
			except pg_exc.ClientCannotConnectError as err:
				for att in connection.failures:
					exc = att.error
					if isinstance(exc, pg_exc.AuthenticationSpecificationError):
						sys.stderr.write(os.linesep + exc.message + (os.linesep*2))
						# keep prompting the user
						need_prompt = True
						connection = None
						break
				else:
					# no invalid password failures..
					raise

		pythonexec = pycmd.Execution(ca,
			context = getattr(co, 'python_context', None),
			loader = getattr(co, 'python_main', None),
		)

		builtin_overload = {
		# New built-ins
			'connector' : connector,
			'db' : connection,
			'do' : connection.do,
			'prepare' : connection.prepare,

			'sqlexec' : connection.execute,
			'settings' : connection.settings,
			'proc' : connection.proc,
			'xact' : connection.xact,
		}
		if not isinstance(__builtins__, dict):
			builtins_d = __builtins__.__dict__
		else:
			builtins_d = __builtins__
		restore = {k : builtins_d.get(k) for k in builtin_overload}

		builtins_d.update(builtin_overload)
		try:
			with connection:
				rv = pythonexec(
					context = pycmd.postmortem(os.environ.get('PYTHON_POSTMORTEM'))
				)
				exc = getattr(sys, 'last_type', None)
				if rv and exc and not issubclass(exc, Exception):
					# Don't try to close it if wasn't an Exception.
					del connection.pq.socket
		finally:
			# restore __builtins__
			builtins_d.update(restore)
			for k, v in builtin_overload.items():
				if v is None:
					del builtins_d[x]
			if trace_file is not None:
				trace_file.close()
	except:
		pg_sys.libpath.remove(os.path.curdir)
		raise
	return rv

if __name__ == '__main__':
	sys.exit(command(sys.argv))
##
# vim: ts=3:sw=3:noet:
