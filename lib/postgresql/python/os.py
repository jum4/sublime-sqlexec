##
# .python.os
##
"""
General OS abstractions and information.
"""
import sys
import os

#: By default, close the FDs on subprocess.Popen().
close_fds = True

#: By default, there is no modification for executable references.
platform_exe = str

def find_file(basename, paths,
	join = os.path.join, exists = os.path.exists,
):
	"""
	Find the file in the given paths. Return the first path
	that exists.
	"""
	for x in paths:
		path = join(x, basename)
		if exists(path):
			return path

if sys.platform in ('win32','win64'):
	# replace variants for windows
	from .msw import close_fds, platform_exe

def find_executable(basename, pathsep = os.pathsep, platexe = platform_exe):
	paths = os.environ.get('PATH', '').split(pathsep)
	return find_file(platexe(basename), paths)
