##
# .versionstring
##
"""
PostgreSQL version parsing.

>>> postgresql.version.split('8.0.1')
(8, 0, 1, None, None)
"""

def split(vstr : str) -> (
	'major','minor','patch',...,'state_class','state_level'
):
	"""
	Split a PostgreSQL version string into a tuple
	(major,minor,patch,...,state_class,state_level)
	"""
	v = vstr.strip().split('.')

	# Get rid of the numbers around the state_class (beta,a,dev,alpha, etc)
	state_class = v[-1].strip('0123456789')
	if state_class:
		last_version, state_level = v[-1].split(state_class)
		if not state_level:
			state_level = None
		else:
			state_level = int(state_level)
		vlist = [int(x or '0') for x in v[:-1]]
		if last_version:
			vlist.append(int(last_version))
		vlist += [None] * (3 - len(vlist))
		vlist += [state_class, state_level]
	else:
		state_level = None
		state_class = None
		vlist = [int(x or '0') for x in v]
		# pad the difference with `None` objects, and +2 for the state_*.
		vlist += [None] * ((3 - len(vlist)) + 2)
	return tuple(vlist)

def unsplit(vtup : tuple) -> str:
	'join a version tuple back into the original version string'
	svtup = [str(x) for x in vtup[:-2] if x is not None]
	state_class, state_level = vtup[-2:]
	return '.'.join(svtup) + (
		'' if state_class is None else state_class + str(state_level)
	)

def normalize(split_version : "a tuple returned by `split`") -> tuple:
	"""
	Given a tuple produced by `split`, normalize the `None` objects into int(0)
	or 'final' if it's the ``state_class``
	"""
	(*head, state_class, state_level) = split_version
	mmp = [x if x is not None else 0 for x in head]
	return tuple(
		mmp + [state_class or 'final', state_level or 0]
	)

default_state_class_priority = [
	'dev',
	'a',
	'alpha',
	'b',
	'beta',
	'rc',
	'final',
	None,
]

python = repr

def xml(self):
	return '<version type="one">\n' + \
		' <major>' + str(self[0]) + '</major>\n' + \
		' <minor>' + str(self[1]) + '</minor>\n' + \
		' <patch>' + str(self[2]) + '</patch>\n' + \
		' <state>' + str(self[-2]) + '</state>\n' + \
		' <level>' + str(self[-1]) + '</level>\n' + \
		'</version>'

def sh(self):
	return """PG_VERSION_MAJOR=%s
PG_VERSION_MINOR=%s
PG_VERSION_PATCH=%s
PG_VERSION_STATE=%s
PG_VERSION_LEVEL=%s""" %(
		str(self[0]),
		str(self[1]),
		str(self[2]),
		str(self[-2]),
		str(self[-1]),
	)

if __name__ == '__main__':
	import sys
	import os
	from optparse import OptionParser
	op = OptionParser()
	op.add_option('-f', '--format',
		type='choice',
		dest='format',
		help='format of output information',
		choices=('sh', 'xml', 'python'),
		default='sh',
	)
	op.add_option('-n', '--normalize',
		action='store_true',
		dest='normalize',
		help='replace missing values with defaults',
		default=False,
	)
	op.set_usage(op.get_usage().strip() + ' "version to parse"')
	co, ca = op.parse_args()
	if len(ca) != 1:
		op.error('requires exactly one argument, the version')
	else:
		v = split(ca[0])
	if co.normalize:
		v = normalize(v)
	sys.stdout.write(getattr(sys.modules[__name__], co.format)(v))
	sys.stdout.write(os.linesep)
