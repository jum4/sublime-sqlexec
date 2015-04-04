# -*- encoding: utf-8 -*-
##
# copyright 2008, James William Pye. http://jwp.name
##
"""
Split, unsplit, parse, serialize, construct and structure resource indicators.

Resource indicators take the form::

  [scheme:[//]][user[:pass]@]host[:port][/[path[/path]*][?param-n1=value[&param-n=value-n]*][#fragment]]

It might be an URL, URI, or IRI. It tries not to care.
Notably, it only percent-encodes chr(0-33) as some RIs support character values
greater than 127. Usually, it's best to make a second pass on the string in
order to target a specific format, URI or IRI.

If a specific format is being targeted, URL or URI or URI-represention of an
IRI, a second pass *must* be made on the string.
# Future versions may include subsequent transformation routines for targeting.

Overview
--------

Where ``x`` is a text RI(ie, ``http://foo.com/path``)::

	unsplit(split(x)) == x
	serialize(parse(x)) == x
	parse(x) == structure(split(x))
	construct(parse(x)) == split(x)


Substructure
------------

In some cases, an RI may have additional structure that needs to be extracted.
To do this, the ``fieldproc`` keyword is used on `split_netloc`, `structure`,
and `parse` functions.

The ``fieldproc`` keyword is a callable that takes a single argument and returns
the processed field. By default, ``fieldproc`` is the `unescape` function which
will decode percent escapes. This is not desirable when substructure exists
within an RI's component as it can create ambiguity about a token when a
percent encoded variant is decoded.
"""
import re

pct_encode = '%%%0.2X'.__mod__
unescaped = '%' + ''.join([chr(x) for x in range(0, 33)])

percent_escapes_re = re.compile('(%[0-9a-fA-F]{2,2})+')
escape_re = re.compile('[%s]' %(re.escape(unescaped),))
escape_user_re = re.compile('[%s]' %(re.escape(unescaped + ':@/?#'),))
escape_password_re = re.compile('[%s]' %(re.escape(unescaped + '@/?#'),))
escape_host_re = re.compile('[%s]' %(re.escape(unescaped + '/?#'),))
escape_port_re = re.compile('[%s]' %(re.escape(unescaped + '/?#'),))
escape_path_re = re.compile('[%s]' %(re.escape(unescaped + '/?#'),))
escape_query_key_re = re.compile('[%s]' %(re.escape(unescaped + '&=#'),))
escape_query_value_re = re.compile('[%s]' %(re.escape(unescaped + '&#'),))

percent_escapes = {}
for x in range(256):
	k = '%0.2X'.__mod__(x)
	percent_escapes[k] = x
	percent_escapes[k.lower()] = x
	percent_escapes[k[0].lower() + k[1]] = x
	percent_escapes[k[0] + k[1].lower()] = x

scheme_chars = '-.+0123456789'
del x

def unescape(x, mkval = chr):
	'Substitute percent escapes with literal characters'
	nstr = type(x)('')
	if isinstance(x, str):
		mkval = chr
	pos = 0
	end = len(x)
	while pos != end:
		newpos = x.find('%', pos)
		if newpos == -1:
			nstr += x[pos:]
			break
		else:
			nstr += x[pos:newpos]

		val = percent_escapes.get(x[newpos+1:newpos+3])
		if val is not None:
			nstr += mkval(val)
			pos = newpos + 3
		else:
			nstr += '%'
			pos = newpos + 1
	return nstr

def re_pct_encode(m):
	return pct_encode(ord(m.group(0)))	

indexes = {
	'scheme' : 0,
	'netloc' : 1,
	'path' : 2,
	'query' : 3,
	'fragment' : 4
}

def split(s):
	"""
	Split an IRI into its base components based on the markers::

		://, /, ?, #

	Return a 5-tuple: (scheme, netloc, path, query, fragment)
	"""
	scheme = None
	netloc = None
	path = None
	query = None
	fragment = None

	end = len(s)
	pos = 0

	# Non-iauthority RI's should be special cased by the user.
	scheme_pos = s.find('://')
	if scheme_pos != -1:
		pos = scheme_pos + 3
		scheme = s[:scheme_pos]
		for x in scheme:
			if not (x in scheme_chars) and \
			not ('A' <= x <= 'Z') and not ('a' <= x <= 'z'):
				pos = 0
				scheme = None
				break

	end_of_netloc = end

	path_pos = s.find('/', pos)
	if path_pos == -1:
		path_pos = None
	else:
		end_of_netloc = path_pos

	query_pos = s.find('?', pos)
	if query_pos == -1:
		query_pos = None
	elif path_pos is None or query_pos < path_pos:
		path_pos = None
		end_of_netloc = query_pos

	fragment_pos = s.find('#', pos)
	if fragment_pos == -1:
		fragment_pos = None
	else:
		if query_pos is not None and fragment_pos < query_pos:
			query_pos = None
		if path_pos is not None and fragment_pos < path_pos:
			path_pos = None
			end_of_netloc = fragment_pos
		if query_pos is None and path_pos is None:
			end_of_netloc = fragment_pos

	if end_of_netloc != pos:
		netloc = s[pos:end_of_netloc]

	if path_pos is not None:
		path = s[path_pos+1:query_pos or fragment_pos or end]

	if query_pos is not None:
		query = s[query_pos+1:fragment_pos or end]

	if fragment_pos is not None:
		fragment = s[fragment_pos+1:end]

	return (scheme, netloc, path, query, fragment)

def unsplit_path(p, re = escape_path_re):
	"""
	Join a list of paths(strings) on "/" *after* escaping them.
	"""
	if not p:
		return None
	return '/'.join([re.sub(re_pct_encode, x) for x in p])

def split_path(p, fieldproc = unescape):
	"""
	Return a list of unescaped strings split on "/".

	Set `fieldproc` to `str` if the components' percent escapes should not be
	decoded.
	"""
	if p is None:
		return []
	return [fieldproc(x) for x in p.split('/')]

def unsplit(t):
	'Make a RI from a split RI(5-tuple)'
	s = ''
	if t[0] is not None:
		s += t[0]
		s += '://'
	if t[1] is not None:
		s += t[1]
	if t[2] is not None:
		s += '/'
		s += t[2]
	if t[3] is not None:
		s += '?'
		s += t[3]
	if t[4] is not None:
		s += '#'
		s += t[4]
	return s

def split_netloc(netloc, fieldproc = unescape):
	"""
	Split a net location into a 4-tuple, (user, password, host, port).

	Set `fieldproc` to `str` if the components' percent escapes should not be
	decoded.
	"""
	pos = netloc.find('@')
	if pos == -1:
		# No user information
		pos = 0
		user = None
		password = None
	else:
		s = netloc[:pos]
		userpw = s.split(':', 1)
		if len(userpw) == 2:
			user, password = userpw
			user = fieldproc(user)
			password = fieldproc(password)
		else:
			user = fieldproc(userpw[0])
			password = None
		pos += 1

	if pos >= len(netloc):
		return (user, password, None, None)

	pos_chr = netloc[pos]
	if pos_chr == '[':
		# IPvN addr
		next_pos = netloc.find(']', pos)
		if next_pos == -1:
			# unterminated IPvN block
			next_pos = len(netloc) - 1
		addr = netloc[pos:next_pos+1]
		pos = next_pos + 1
		next_pos = netloc.find(':', pos)
		if next_pos == -1:
			port = None
		else:
			port = fieldproc(netloc[next_pos+1:])
	else:
		next_pos = netloc.find(':', pos)
		if next_pos == -1:
			addr = fieldproc(netloc[pos:])
			port = None
		else:
			addr = fieldproc(netloc[pos:next_pos])
			port = fieldproc(netloc[next_pos+1:])

	return (user, password, addr, port)

def unsplit_netloc(t):
	'Create a netloc fragment from the given tuple(user,password,host,port)'
	if t[0] is None and t[2] is None:
		return None
	s = ''
	if t[0] is not None:
		s += escape_user_re.sub(re_pct_encode, t[0])
		if t[1] is not None:
			s += ':'
			s += escape_password_re.sub(re_pct_encode, t[1])
		s += '@'

	if t[2] is not None:
		s += escape_host_re.sub(re_pct_encode, t[2])
		if t[3] is not None:
			s += ':'
			s += escape_port_re.sub(re_pct_encode, t[3])

	return s

def structure(t, fieldproc = unescape):
	"""
	Create a dictionary from a split RI(5-tuple).

	Set `fieldproc` to `str` if the components' percent escapes should not be
	decoded.
	"""
	d = {}

	if t[0] is not None:
		d['scheme'] = t[0]

	if t[1] is not None:
		uphp = split_netloc(t[1], fieldproc = fieldproc)
		if uphp[0] is not None:
			d['user'] = uphp[0]
		if uphp[1] is not None:
			d['password'] = uphp[1]
		if uphp[2] is not None:
			d['host'] = uphp[2]
		if uphp[3] is not None:
			d['port'] = uphp[3]

	if t[2] is not None:
		if t[2]:
			d['path'] = list(map(fieldproc, t[2].split('/')))
		else:
			d['path'] = []

	if t[3] is not None:
		if t[3]:
			d['query'] = [tuple((list(map(fieldproc, x.split('=', 1))) + [None])[:2]) for x in t[3].split('&')]
		else:
			# no characters followed the '?'
			d['query'] = []

	if t[4] is not None:
		d['fragment'] = fieldproc(t[4])
	return d

def construct_query(x,
	key_re = escape_query_key_re,
	value_re = escape_query_value_re,
):
	'Given a sequence of (key, value) pairs, construct'
	return '&'.join([
		v is not None and \
		'%s=%s' %(
			key_re.sub(re_pct_encode, k),
			value_re.sub(re_pct_encode, v),
		) or \
		key_re.sub(re_pct_encode, k)
		for k, v in x
	])

def construct(x):
	'Construct a RI tuple(5-tuple) from a dictionary object'
	p = x.get('path')
	if p is not None:
		p = '/'.join([escape_path_re.sub(re_pct_encode, y) for y in p])
	q = x.get('query')
	if q is not None:
		q = construct_query(q)
	f = x.get('fragment')
	if f is not None:
		f = escape_re.sub(re_pct_encode, f)

	u = x.get('user')
	pw = x.get('password')
	h = x.get('host')
	port = x.get('port')

	return (
		x.get('scheme'),
		# netloc: [user[:pass]@]host[:port]
		unsplit_netloc((
			x.get('user'),
			x.get('password'),
			x.get('host'),
			x.get('port'),
		)),
		p, q, f
	)

def parse(s, fieldproc = unescape):
	"""
	Parse an RI into a dictionary object. Synonym for ``structure(split(x))``.

	Set `fieldproc` to `str` if the components' percent escapes should not be
	decoded.
	"""
	return structure(split(s), fieldproc = fieldproc)

def serialize(x):
	'Return an RI from a dictionary object. Synonym for ``unsplit(construct(x))``'
	return unsplit(construct(x))

__docformat__ = 'reStructuredText'
