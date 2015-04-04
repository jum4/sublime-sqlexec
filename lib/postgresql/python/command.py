##
# .python.command - Python command emulation module.
##
"""
Create and Execute Python Commands
==================================

The purpose of this module is to simplify the creation of a Python command
interface. Normally, one would want to do this if there is a *common* need
for a certain Python environment that may be, at least, partially initialized
via command line options. A notable case would be a Python environment with a
database connection whose connection parameters came from the command line. That
is, Python + command line driven configuration.

The module also provides an extended interactive console that provides backslash
commands for editing and executing temporary files. Use ``python -m
pythoncommand`` to try it out.

Simple usage::

	import sys
	import os
	import optparse
	import pythoncommand as pycmd

	op = optparse.OptionParser(
		"%prog [options] [script] [script arguments]",
		version = '1.0',
	)
	op.disable_interspersed_args()

	# Basically, the standard -m and -c. (Some additional ones for fun)
	op.add_options(pycmd.default_optparse_options)

	co, ca = op.parse_args(args[1:])

	# This initializes an execution instance which gathers all the information
	# about the code to be ran when ``pyexe`` is called.
	pyexe = pycmd.Execution(ca,
		context = getattr(co, 'python_context', ()),
		loader = getattr(co, 'python_main', None),
	)

	# And run it. Any exceptions will be printed via print_exception.
	rv = pyexe()
	sys.exit(rv)
"""
import os
import sys
import re
import code
import types
import optparse
import subprocess
import contextlib

from gettext import gettext as _
from traceback import print_exception

from pkgutil import get_loader as module_loader

class single_loader(object):
	"""
	used for "loading" string modules(think -c)
	"""
	def __init__(self, source):
		self.source = source

	def get_filename(self, fullpath):
		if fullpath == self.source:
			return '<command>'

	def get_code(self, fullpath):
		if fullpath == self.source:
			return compile(self.source, '<command>', 'exec')

	def get_source(self, fullpath):
		if fullpath == self.source:
			return self.source

class file_loader(object):
	"""
	used for "loading" scripts
	"""
	def __init__(self, filepath, fileobj = None):
		self.filepath = filepath
		if fileobj is not None:
			self._source = fileobj.read()

	def get_filename(self, fullpath):
		if fullpath == self.filepath:
			return self.filepath

	def get_source(self, fullpath):
		if fullpath == self.filepath:
			return self._read()

	def _read(self):
		if hasattr(self, '_source'):
			return self._source
		f = open(self.filepath)
		try:
			return f.read()
		finally:
			f.close()

	def get_code(self, fullpath):
		if fullpath != self.filepath:
			return
		return compile(self._read(), self.filepath, 'exec')

def extract_filepath(x):
	if x.startswith('file://'):
		return x[7:]
	return None

def extract_module(x):
	if x.startswith('module:'):
		return x[7:]
	return None

module_loader_descriptor = (
	'Python module', module_loader, extract_module
)
file_loader_descriptor = (
	'Python script', file_loader, extract_filepath
)
single_loader_descriptor = (
	'Python command', single_loader, lambda x: x
)

_directory = (
	module_loader_descriptor,
	file_loader_descriptor,
)
directory = list(_directory)

def find_loader(ident, dir = directory):
	for x in dir:
		xid = x[2](ident)
		if xid is not None:
			return x

##
# optparse options
##

def append_context(option, opt_str, value, parser):
	"""
	Add some context to the execution of the Python code using
	loader module's directory list of loader descriptions.

	If no loader can be found, assume it's a Python command.
	"""
	pc = getattr(parser.values, option.dest, None) or []
	if not pc:
		setattr(parser.values, option.dest, pc)
	ldesc = find_loader(value)
	if ldesc is None:
		ldesc = single_loader_descriptor
	pc.append((value, ldesc))

def set_python_main(option, opt_str, value, parser):
	"""
	Set the main Python code; after contexts are initialized, main is ran.
	"""
	main = (value, option.python_loader)
	setattr(parser.values, option.dest, main)
	# only terminate parsing if not interspersing arguments
	if not parser.allow_interspersed_args:
		parser.rargs.insert(0, '--')

context = optparse.make_option(
	'-C', '--context',
	help = _('Python context code to run[file://,module:,<code>]'),
	dest = 'python_context',
	action = 'callback',
	callback = append_context,
	type = 'str'
)

module = optparse.make_option(
	'-m',
	help = _('Python module to run as script(__main__)'),
	dest = 'python_main',
	action = 'callback',
	callback = set_python_main,
	type = 'str'
)
module.python_loader = module_loader_descriptor

command = optparse.make_option(
	'-c',
	help = _('Python expression to run(__main__)'),
	dest = 'python_main',
	action = 'callback',
	callback = set_python_main,
	type = 'str'
)
command.python_loader = single_loader_descriptor

default_optparse_options = [
	context, module, command,
]

class ExtendedConsole(code.InteractiveConsole):
	"""
	Console subclass providing some convenient backslash commands.
	"""
	def __init__(self, *args, **kw):
		import tempfile
		self.mktemp = tempfile.mktemp
		import shlex
		self.split = shlex.split
		code.InteractiveConsole.__init__(self, *args, **kw)

		self.bsc_map = {}
		self.temp_files = {}
		self.past_buffers = []

		self.register_backslash(r'\?', self.showhelp, "Show this help message.")
		self.register_backslash(r'\set', self.bs_set,
			"Configure environment variables. \set without arguments to show all")
		self.register_backslash(r'\E', self.bs_E,
			"Edit a file or a temporary script.")
		self.register_backslash(r'\i', self.bs_i,
			"Execute a Python script within the interpreter's context.")
		self.register_backslash(r'\e', self.bs_e,
			"Edit and Execute the file directly in the context.")
		self.register_backslash(r'\x', self.bs_x,
			"Execute the Python command within this process.")

	def interact(self, *args, **kw):
		self.showhelp(None, None)
		return super().interact(*args,**kw)

	def showtraceback(self):
		e, v, tb = sys.exc_info()
		sys.last_type, sys.last_value, sys.last_traceback = e, v, tb
		print_exception(e, v, tb.tb_next or tb)

	def register_backslash(self, bscmd, meth, doc):
		self.bsc_map[bscmd] = (meth, doc)

	def execslash(self, line):
		"""
		If push() gets a line that starts with a backslash, execute
		the command that the backslash sequence corresponds to.
		"""
		cmd = line.split(None, 1)
		cmd.append('')
		bsc = self.bsc_map.get(cmd[0])
		if bsc is None:
			self.write("ERROR: unknown backslash command: %s%s"%(cmd, os.linesep))
		else:
			return bsc[0](cmd[0], cmd[1])

	def showhelp(self, cmd, arg):
		i = list(self.bsc_map.items())
		i.sort(key = lambda x: x[0])
		helplines = os.linesep.join([
			'  %s%s%s' %(
				x[0], ' ' * (8 - len(x[0])), x[1][1]
			) for x in i
		])
		self.write("Backslash Commands:%s%s%s" %(
			os.linesep*2, helplines, os.linesep*2
		))

	def bs_set(self, cmd, arg):
		"""
		Set a value in the interpreter's environment.
		"""
		if arg:
			for x in self.split(arg):
				if '=' in x:
					k, v = x.split('=', 1)
					os.environ[k] = v
					self.write("%s=%s%s" %(k, v, os.linesep))
				elif x:
					self.write("%s=%s%s" %(x, os.environ.get(x, ''), os.linesep))
		else:
			for k,v in os.environ.items():
				self.write("%s=%s%s" %(k, v, os.linesep))

	def resolve_path(self, path, dont_create = False):
		"""
		Get the path of the given string; if the path is not
		absolute and does not contain path separators, identify
		it as a temporary file.
		"""
		if not os.path.isabs(path) and not os.path.sep in path:
			# clean it up to avoid typos
			path = path.strip().lower()
			tmppath = self.temp_files.get(path)
			if tmppath is None:
				if dont_create is False:
					tmppath = self.mktemp(
						suffix = '.py',
						prefix = '_console_%s_' %(path,)
					)
					self.temp_files[path] = tmppath
				else:
					return path
			return tmppath
		return path

	def execfile(self, filepath):
		src = open(filepath)
		try:
			try:
				co = compile(src.read(), filepath, 'exec')
			except SyntaxError:
				co = None
				print_exception(*sys.exc_info())
		finally:
			src.close()
		if co is not None:
			try:
				exec(co, self.locals, self.locals)
			except:
				e, v, tb = sys.exc_info()
				print_exception(e, v, tb.tb_next or tb)

	def editfiles(self, filepaths):
		sp = list(filepaths)
		# ;)
		sp.insert(0, os.environ.get('EDITOR', 'vi'))
		return subprocess.call(sp)

	def bs_i(self, cmd, arg):
		'execute the files'
		for x in self.split(arg) or ('',):
			p = self.resolve_path(x, dont_create = True)
			self.execfile(p)

	def bs_E(self, cmd, arg):
		'edit the files, but *only* edit them'
		self.editfiles([self.resolve_path(x) for x in self.split(arg) or ('',)])

	def bs_e(self, cmd, arg):
		'edit *and* execute the files'
		filepaths = [self.resolve_path(x) for x in self.split(arg) or ('',)]
		self.editfiles(filepaths)
		for x in filepaths:
			self.execfile(x)

	def bs_x(self, cmd, arg):
		rv = -1
		if len(cmd) > 1:
			a = self.split(arg)
			a.insert(0, '\\x')
			try:
				rv = command(argv = a)
			except SystemExit as se:
				rv = se.code
			self.write("[Return Value: %d]%s" %(rv, os.linesep))

	def push(self, line):
		# Has to be a ps1 context.
		if not self.buffer and line.startswith('\\'):
			try:
				self.execslash(line)
			except:
				# print the exception, but don't raise.
				e, v, tb = sys.exc_info()
				print_exception(e, v, tb.tb_next or tb)
		else:
			return code.InteractiveConsole.push(self, line)

@contextlib.contextmanager
def postmortem(funcpath):
	if not funcpath:
		yield None
	else:
		pm = funcpath.split('.')
		attr = pm.pop(-1)
		modpath = '.'.join(pm)
		try:
			m = __import__(modpath, fromlist = modpath)
			pmobject = getattr(m, attr, None)
		except ValueError:
			pmobject = None

			sys.stderr.write(
				"%sERROR: no object at %r for postmortem%s"%(
					os.linesep, funcpath, os.linesep
				)
			)
		try:
			yield None
		except:
			try:
				sys.last_type, sys.last_value, sys.last_traceback = sys.exc_info()
				pmobject()
			except:
				sys.stderr.write(
					"[Exception raised by Postmortem]" + os.linesep
				)
				print_exception(*sys.exc_info())
			raise

class Execution(object):
	"""
	Given argv and context make an execution instance that, when called, will
	execute the configured Python code.

	This class provides the ability to identify what the main part of the
	execution of the configured Python code. For instance, shall it execute a
	console, the file that the first argument points to, a -m option module
	appended to the python_context option value, or the code given within -c?
	"""
	def __init__(self,
		args, context = (),
		main = None,
		loader = None,
		stdin = sys.stdin
	):
		"""
		args
			The arguments passed to the script; usually sys.argv after being
			processed by optparse(ca).
		context
			A list of loader descriptors that will be used to establish the
			context of __main__ module.
		main
			Overload to explicitly state what main is. None will cause the
			class to attempt to fill in the attribute using 'args' and other
			system objects like sys.stdin.
		"""
		self.args = args
		self.context = context and list(context) or ()

		if main is not None:
			self.main = main
		elif loader is not None:
			# Main explicitly stated, resolve the path and the loader
			path, ldesc = loader
			ltitle, rloader, xpath = ldesc
			l = rloader(path)
			if l is None:
				raise ImportError(
					"%s %r does not exist or cannot be read" %(
						ltitle, path
					)
				)
			self.main = (path, l)
		# If there are args, but no main, run the first arg.
		elif args:
			fp = self.args[0]
			f = open(fp)
			try:
				l = file_loader(fp, fileobj = f)
			finally:
				f.close()
			self.main = (self.args[0], l)
			self.args = self.args[1:]
		# There is no main, no loader, and no args.
		# If stdin is not a tty, use stdin as the main file.
		elif not stdin.isatty():
			l = file_loader('<stdin>', fileobj = stdin)
			self.main = ('<stdin>', l)
		# tty and no "main".
		else:
			# console
			self.main = (None, None)
		self.reset_module__main__()

	def reset_module__main__(self):
		mod = types.ModuleType('__main__')
		mod.__builtins__ = __builtins__
		mod.__package__ = None
		self.module__main__ = mod
		path = getattr(self.main[1], 'fullname', None)
		if path is not None:
			mod.__package__ = '.'.join(path.split('.')[:-1])

	def _call(self,
		console = ExtendedConsole,
		context = None
	):
		"""
		Initialize the context and run main in the given locals
		(Note: tramples on sys.argv, __main__ in sys.modules)
		(Use __call__ instead)
		"""
		sys.modules['__main__'] = self.module__main__
		md = self.module__main__.__dict__

		# Establish execution context in the locals;
		# iterate over all the loaders in self.context and
		for path, ldesc in self.context:
			ltitle, loader, xpath = ldesc
			rpath = xpath(path)
			li = loader(rpath)
			if li is None:
				sys.stderr.write(
					"%s %r does not exist or cannot be read%s" %(
						ltitle, rpath, os.linesep
					)
				)
				return 1
			try:
				code = li.get_code(rpath)
			except:
				print_exception(*sys.exc_info())
				return 1
			self.module__main__.__file__ = getattr(
				li, 'get_filename', lambda x: x
			)(rpath)
			self.module__main__.__loader__ = li
			try:
				exec(code, md, md)
			except:
				e, v, tb = sys.exc_info()
				print_exception(e, v, tb.tb_next or tb)
				return 1

		if self.main == (None, None):
			# It's interactive.
			sys.argv = self.args or ['<console>']

			# Use readline if available
			try:
				import readline
			except ImportError:
				pass

			ic = console(locals = md)
			try:
				ic.interact()
			except SystemExit as e:
				return e.code
			return 0
		else:
			# It's ultimately a code object.
			path, loader = self.main
			self.module__main__.__file__ = getattr(
				loader, 'get_filename', lambda x: x
			)(path)
			sys.argv = list(self.args)
			sys.argv.insert(0, self.module__main__.__file__)
			try:
				code = loader.get_code(path)
			except:
				print_exception(*sys.exc_info())
				return 1

			rv = 0
			exe_exception = False
			try:
				if context is not None:
					with context:
						try:
							exec(code, md, md)
						except:
							exe_exception = True
							raise
				else:
					try:
						exec(code, md, md)
					except:
						exe_exception = True
						raise

			except SystemExit as e:
				# Assume it's an exe_exception as anything ran in `context`
				# shouldn't cause an exception.
				rv = e.code
				e, v, tb = sys.exc_info()
				sys.last_type = e
				sys.last_value = v
				sys.last_traceback = (tb.tb_next or tb)
			except:
				if exe_exception is False:
					raise
				rv = 1
				e, v, tb = sys.exc_info()
				print_exception(e, v, tb.tb_next or tb)
				sys.last_type = e
				sys.last_value = v
				sys.last_traceback = (tb.tb_next or tb)

			return rv

	def __call__(self, *args, **kw):
		storage = (
			sys.modules.get('__context__'),
			sys.modules.get('__main__'),
			sys.argv,
			os.environ.copy(),
		)
		try:
			return self._call(*args, **kw)
		finally:
			sys.modules['__context__'], \
			sys.modules['__main__'], \
			sys.argv, os.environ = storage

	def get_main_source(self):
		"""
		Get the execution's "__main__" source. Useful for configuring
		environmental options derived from "magic" lines.
		"""
		path, loader = self.main
		if path is not None:
			return loader.get_source(path)

def command_execution(argv = sys.argv):
	'create an execution using the given argv'
	# The pwd should be in the path for python commands.
	# setuptools' console_scripts appear to strip this out.
	if '' not in sys.path:
		sys.path.insert(0, '')

	op = optparse.OptionParser(
		"%prog [options] [script] [script arguments]",
		version = '1.0',
	)
	op.disable_interspersed_args()
	op.add_options(default_optparse_options)
	co, ca = op.parse_args(argv[1:])

	return Execution(ca,
		context = getattr(co, 'python_context', ()),
		loader = getattr(co, 'python_main', None),
	)

def command(argv = sys.argv):
	return command_execution(argv = argv)(
		context = postmortem(os.environ.get('PYTHON_POSTMORTEM'))
	)

if __name__ == '__main__':
	sys.exit(command())
##
# vim: ts=3:sw=3:noet:
