##
# .iri
##
"""
Parse and serialize PQ IRIs.

PQ IRIs take the form::

	pq://user:pass@host:port/database?setting=value&setting2=value2#public,othernamespace

IPv6 is supported via the standard representation::

	pq://[::1]:5432/database

Driver Parameters:

	pq://user@host/?[driver_param]=value&[other_param]=value?setting=val
"""
from .resolved import riparse as ri
from .string import split_ident

from operator import itemgetter
get0 = itemgetter(0)
del itemgetter

import re
escape_path_re = re.compile('[%s]' %(re.escape(ri.unescaped + ','),))

def structure(d, fieldproc = ri.unescape):
	'Create a clientparams dictionary from a parsed RI'
	if d.get('scheme', 'pq').lower() != 'pq':
		raise ValueError("PQ-IRI scheme is not 'pq'")
	cpd = {
		k : fieldproc(v) for k, v in d.items()
		if k not in ('path', 'fragment', 'query', 'host', 'scheme')
	}

	path = d.get('path')
	frag = d.get('fragment')
	query = d.get('query')
	host = d.get('host')

	if host is not None:
		if host.startswith('[') and host.endswith(']'):
			host = host[1:-1]
			if host.startswith('unix:'):
				cpd['unix'] = host[len('unix:'):].replace(':','/')
			else:
				cpd['host'] = host[1:-1]
		else:
			cpd['host'] = fieldproc(host)

	if path:
		# Only state the database field's existence if the first path is non-empty.
		if path[0]:
			cpd['database'] = path[0]
		path = path[1:]
		if path:
			cpd['path'] = path

	settings = {}
	if query:
		if hasattr(query, 'items'):
			qiter = query.items()
		else:
			qiter = query
		for k, v in qiter:
			if k.startswith('[') and k.endswith(']'):
				k = k[1:-1]
				if k != 'settings' and k not in cpd:
					cpd[fieldproc(k)] = fieldproc(v)
			elif k:
				settings[fieldproc(k)] = fieldproc(v)
			# else: ignore empty query keys

	if frag:
		settings['search_path'] = [
			fieldproc(x) for x in frag.split(',')
		]

	if settings:
		cpd['settings'] = settings

	return cpd

def construct_path(x, re = escape_path_re):
	"""
	Join a path sequence using ',' and escaping ',' in the pieces.
	"""
	return ','.join((re.sub(ri.re_pct_encode, y) for y in x))

def construct(x, obscure_password = False):
	'Construct a RI dictionary from a clientparams dictionary'
	# the rather exhaustive settings choreography is due to
	# a desire to allow the search_path to be appended in the fragment
	settings = x.get('settings')
	no_path_settings = None
	search_path = None
	if settings:
		if isinstance(settings, dict):
			siter = settings.items()
			search_path = settings.get('search_path')
		else:
			siter = list(settings)
			search_path = [(k,v) for k,v in siter if k == 'search_path']
			search_path.append((None,None))
			search_path = search_path[-1][1]
		no_path_settings = [(k,v) for k,v in siter if k != 'search_path']
		if not no_path_settings:
			no_path_settings = None

	# It could be a string search_path, split if it is.
	if search_path is not None and isinstance(search_path, str):
		search_path = split_ident(search_path, sep = ',')

	port = None
	if 'unix' in x:
		host = '[unix:' + x['unix'].replace('/',':') + ']'
		# ignore port.. it's a mis-config.
	elif 'host' in x:
		host = x['host']
		if ':' in host:
			host = '[' + host + ']'
		port = x.get('port')
	else:
		host = None
		port = x.get('port')

	path = []
	if 'database' in x:
		path.append(x['database'])
	if 'path' in x:
		path.extend(x['path'] or ())

	password = x.get('password')
	if obscure_password and password is not None:
		password = '***'
	driver_params = list({
		'[' + k + ']' : str(v) for k,v in x.items()
		if k not in (
			'user', 'password', 'port', 'database', 'ssl',
			'path', 'host', 'unix', 'ipv','settings'
		)
	}.items())
	driver_params.sort(key=get0)

	return (
		'pqs' if x.get('ssl', False) is True else 'pq',
		# netloc: user:pass@host[:port]
		ri.unsplit_netloc((
			x.get('user'),
			password,
			host,
			None if 'port' not in x else str(x['port'])
		)),
		None if not path else '/'.join([
			ri.escape_path_re.sub(path_comp, '/')
			for path_comp in path
		]),
		(ri.construct_query(driver_params) if driver_params else None)
		if no_path_settings is None else (
			ri.construct_query(
				driver_params + no_path_settings
			)
		),
		None if search_path is None else construct_path(search_path),
	)

def parse(s, fieldproc = ri.unescape):
	'Parse a Postgres IRI into a dictionary object'
	return structure(
		# In ri.parse, don't unescape the parsed values as our sub-structure
		# uses the escape mechanism in IRIs to specify literal separator
		# characters.
		ri.parse(s, fieldproc = str),
		fieldproc = fieldproc
	)

def serialize(x, obscure_password = False):
	'Return a Postgres IRI from a dictionary object.'
	return ri.unsplit(construct(x, obscure_password = obscure_password))

if __name__ == '__main__':
	import sys
	for x in sys.argv[1:]:
		print("{src} -> {parsed!r} -> {serial}".format(
			src = x,
			parsed = parse(x),
			serial = serialize(parse(x))
		))
