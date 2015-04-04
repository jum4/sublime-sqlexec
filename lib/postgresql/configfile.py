##
# .configfile
##
'PostgreSQL configuration file parser and editor functions.'
import sys
import os
from . import string as pg_str
from . import api as pg_api

quote = "'"
comment = '#'

def parse_line(line, equality = '=', comment = comment, quote = quote):
	keyval = line.split(equality, 1)
	if len(keyval) == 2:
		key, val = keyval

		prekey_len = 0
		for c in key:
			if not c.isspace() and c not in comment:
				break
			prekey_len += 1

		key_len = 0
		for c in key[prekey_len:]:
			if not (c.isalpha() or c.isdigit() or c in '_'):
				break
			key_len += 1

		# If non-whitespace exists after the key,
		# it's a complex comment, so just bail out.
		if key[prekey_len + key_len:].strip():
			return

		preval_len = 0
		for c in val:
			if not c.isspace() or c in '\n\r':
				break
			preval_len += 1

		inquotes = False
		escaped = False
		val_len = 0
		for i in range(preval_len, len(val)):
			c = val[i]
			if c == quote:
				if inquotes is False:
					inquotes = True
				else:
					if escaped is False:
						# Peek ahead to see if it's escaped with another quote
						escaped = (len(val) > i+1 and val[i+1] == quote)
						if escaped is False:
							inquotes = False
					elif escaped is True:
						# It *was* an escaped quote.
						escaped = False
			elif inquotes is False and (c.isspace() or c in comment):
				break
			val_len += 1

		return (
			# The key slice
			slice(prekey_len, key_len + prekey_len),
			# The value slice
			slice(len(key) + 1 + preval_len, len(key) + 1 + preval_len + val_len)
		)

def unquote(s, quote = quote):
	"""
	Unquote the string `s` if quoted.
	"""
	s = s.strip()
	if not s.startswith(quote):
		return s
	return s[1:-1].replace(quote*2, quote)

def write_config(map, writer, keys = None):
	'A configuration writer that will trample & merely write the settings'
	if keys is None:
		keys = map
	for k in keys:
		writer('='.join((k, map[k])) + os.linesep)

def alter_config(
	map : "the configuration changes to make",
	fo : "file object containing configuration lines(Iterable)",
	keys : "the keys to change; defaults to map.keys()" = None
):
	'Alters a configuration file without trampling on the existing structure'
	if keys is None:
		keys = list(map.keys())
	# Normalize keys and map them back to
	pkeys = {
		k.lower().strip() : keys.index(k) for k in keys
	}

	lines = []
	candidates = {}
	i = -1
	# Process lines in fo
	for l in fo:
		i += 1
		lines.append(l)
		pl = parse_line(l)
		# "Bad" line? fuh-get-duh-bowt-it.
		if pl is None:
			continue
		sk, sv = pl
		k = l[sk].lower()
		v = l[sv]
		# It's a candidate?
		if k in pkeys:
			c = candidates.get(k)
			if c is None:
				candidates[k] = c = []
			c.append((i, sk, sv))
	# Simply insert the data somewhere for unfound keys.
	for k in pkeys:
		if k not in candidates:
			key = keys[pkeys[k]]
			val = map[key]
			# Can't comment without an uncommented candidate.
			if val is not None:
				if not lines[-1].endswith(os.linesep):
					lines[-1] = lines[-1] + os.linesep
				lines.append("%s = '%s'" %(key, val.replace("'", "''")))

	# Multiple lines may have the key, so make a decision based on the value.
	for ck in candidates.keys():
		to_set_key = keys[pkeys[ck]]
		to_set_val = map[keys[pkeys[ck]]]

		if to_set_val is None:
			# Comment uncommented occurrences.
			for cl in candidates[ck]:
				line_num, sk, sv = cl
				if comment not in lines[line_num][:sk.start]:
					lines[line_num] = '#' + lines[line_num]
		else:
			# Manage occurrences.
			# w_ is for winner.
			# Now, a winner is elected for alteration. The winner
			# is decided based on a two factors: commenting and value.
			w_score = -1
			w_commented = None
			w_val = None
			w_cl = None
			for cl in candidates[ck]:
				line_num, sk, sv = cl
				l = lines[line_num]
				lkey = l[sk]
				lval = l[sv]
				commented = (comment in l[:sk.start])
				score = \
					(not commented and 1 or 0) + \
					(unquote(lval) == to_set_val and 2 or 0)
				# So, if a line is not commented, and has equal
				# values, then that's the winner. If a line is commented,
				# and has a has equal values, it will succeed over a mere
				# uncommented value.

				if score > w_score:
					if w_commented is False:
						# It's now a loser, so comment it out if necessary.
						lines[w_cl[0]] = '#' + lines[w_cl[0]]
					w_score = score
					w_commented = commented
					w_val = lval
					w_cl = cl
				elif commented is False:
					# Loser needs to be commented.
					lines[line_num] = '#' + l

			line_num, sk, sv = w_cl
			l = lines[line_num]
			if w_commented:
				bol = ''
			else:
				bol = l[:sk.start]
			post_val = l[sv.stop:]
			# If there is post-value data, validate that it's commented.
			if post_val and not post_val.isspace():
				stripped_post_val = post_val.lstrip()
				if not stripped_post_val.startswith(comment):
					post_val = '%s%s%s' %(
						# The whitespace before the uncommented visibles
						post_val[0:len(post_val) - len(stripped_post_val)],
						# A comment followed by the uncommented visibles
						comment, stripped_post_val
					)
			# Everything is set as quoted as it's the only safe
			# way to set something without delving into setting types.
			lines[line_num] = \
				bol + l[sk.start:sv.start] + \
				"'%s'" %(to_set_val.replace("'", "''"),) + post_val
	return lines

def read_config(iter, d = None, selector = None):
	if d is None:
		d = {}
	for line in iter:
		kv = parse_line(line)
		if kv:
			key = line[kv[0]]
			if comment not in line[:kv[0].start] and \
			(selector is None or selector(key)):
				d[key] = unquote(line[kv[1]])
	return d

class ConfigFile(pg_api.Settings):
	"""
	Provides a mapping interface to a configuration file.

	Every action will cause the file to be wholly read, so using `update` to make
	multiple changes is desirable.
	"""
	_e_factors = ('path',)
	_e_label = 'CONFIGFILE'

	def _e_metas(self):
		yield (None, len(self.keys()))

	def __init__(self, path, open = open):
		self.path = path
		self._open = open
		self._store = []
		self._restore = {}

	def __repr__(self):
		return "%s.%s(%r)" %(
			type(self).__module__,
			type(self).__name__,
			self.path
		)

	def _save(self, lines : [str]):
		with self._open(self.path, 'w') as cf:
			for l in lines:
				cf.write(l)

	def __delitem__(self, k):
		with self._open(self.path) as cf:
			lines = alter_config({k : None}, cf)
		self._save()

	def __getitem__(self, k):
		with self._open(self.path) as cfo:
			return read_config(
				cfo,
				selector = k.__eq__
			)[k]

	def __setitem__(self, k, v):
		self.update({k : v})

	def __call__(self, **kw):
		self._store.insert(0, kw)

	def __context__(self):
		return self

	def __iter__(self):
		return self.keys()

	def __len__(self):
		return len(list(self.keys()))

	def __enter__(self):
		res = self.getset(self._store[0].keys())
		self.update(self._store[0])
		del self._store[0]
		self._restore.append(res)

	def __exit__(self, exc, val, tb):
		self._restored.update(self._restore[-1])
		del self._restore[-1]
		self.update(self._restored)
		self._restored.clear()
		return exc is None

	def get(self, k, alt = None):
		with self._open(self.path) as cf:
			return read_config(cf, selector = k.__eq__).get(k, alt)

	def keys(self):
		return read_config(self._open(self.path)).keys()

	def values(self):
		return read_config(self._open(self.path)).values()

	def items(self):
		return read_config(self._open(self.path)).items()

	def update(self, keyvals):
		"""
		Given a dictionary of settings, apply them to the cluster's
		postgresql.conf.
		"""
		with self._open(self.path) as cf:
			lines = alter_config(keyvals, cf)
		self._save(lines)

	def getset(self, keys):
		"""
		Get all the settings in the list of keys.
		Returns a dictionary of those keys.
		"""
		keys = set(keys)
		with self._open(self.path) as cfo:
			cfg = read_config(
				cfo,
				selector = keys.__contains__
			)
			for x in (keys - set(cfg.keys())):
				cfg[x] = None
			return cfg
##
# vim: ts=3:sw=3:noet:
