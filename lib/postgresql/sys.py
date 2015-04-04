##
# .sys
##
"""
py-postgresql system functions and data.

Data
----

 ``libpath``
  The local file system paths that contain query libraries.

Overridable Functions
---------------------

 excformat
  Information that makes up an exception's displayed "body".
  Effectively, the implementation of `postgresql.exception.Error.__str__`

 msghook
  Display a message.
"""
import sys
import os
import traceback
from .python.element import format_element
from .python.string import indent

libpath = []

def default_errformat(val):
	"""
	Built-in error formatter. DON'T TOUCH!
	"""
	it = val._e_metas()
	if val.creator is not None:
		# Protect against element traceback failures.
		try:
			after = os.linesep + format_element(val.creator)
		except Exception:
			after = 'Element Traceback of %r caused exception:%s' %(
				type(val.creator).__name__,
				os.linesep
			)
			after += indent(traceback.format_exc())
			after = os.linesep + indent(after).rstrip()
	else:
		after = ''
	return next(it)[1] \
		+ os.linesep + '  ' \
		+ (os.linesep + '  ').join(
			k + ': ' + v for k, v in it
		) + after

def default_msghook(msg, format_message = format_element):
	"""
	Built-in message hook. DON'T TOUCH!
	"""
	if sys.stderr and not sys.stderr.closed:
		try:
			sys.stderr.write(format_message(msg) + os.linesep)
		except Exception:
			try:
				sys.excepthook(*sys.exc_info())
			except Exception:
				# gasp.
				pass

def errformat(*args, **kw):
	"""
	Raised Database Error formatter pointing to default_excformat.

	Override if you like. All postgresql.exceptions.Error's are formatted using
	this function.
	"""
	return default_errformat(*args, **kw)

def msghook(*args, **kw):
	"""
	Message hook pointing to default_msghook.

	Override if you like. All untrapped messages raised by
	driver connections come here to be printed to stderr.
	"""
	return default_msghook(*args, **kw)

def reset_errformat(with_func = errformat):
	'restore the original excformat function'
	global errformat
	errformat = with_func

def reset_msghook(with_func = msghook):
	'restore the original msghook function'
	global msghook
	msghook = with_func
