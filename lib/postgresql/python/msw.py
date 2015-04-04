##
# .python.msw
##
"""
Additional Microsoft Windows tools.
"""

# for Popen(), not supported on windows
close_fds = False

def platform_exe(name):
	"""
	Append '.exe' if it's not already there.
	"""
	if name.endswith('.exe'):
		return name
	return name + '.exe'
