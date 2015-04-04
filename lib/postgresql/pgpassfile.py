##
# .pgpassfile - parse and lookup passwords in a pgpassfile
##
'Parse pgpass files and subsequently lookup a password.'
from os.path import exists

def split(line, len = len):
	line = line.strip()
	if not line:
		return None
	r = []
	continuation = False
	for x in line.split(':'):
		if continuation:
			# The colon was preceded by a backslash, it's part
			# of the last field. Substitute the trailing backslash
			# with the colon and append the next value.
			r[-1] = r[-1][:-1] + ':' + x.replace('\\\\', '\\')
			continuation = False
		else:
			# Even number of backslashes preceded the split.
			# Normal field.
			r.append(x.replace('\\\\', '\\'))
		# Determine if the next field is a continuation of this one.
		if (len(x) - len(x.rstrip('\\'))) % 2 == 1:
			continuation = True
	if len(r) != 5:
		# Too few or too many fields.
		return None
	return r

def parse(data):
	'produce a list of [(word, (host,port,dbname,user))] from a pgpass file object'
	return [
		(x[-1], x[0:4]) for x in [split(line) for line in data] if x
	]

def lookup_password(words, uhpd):
	"""
	lookup_password(words, (user, host, port, database)) -> password

	Where 'words' is the output from pgpass.parse()
	"""
	user, host, port, database = uhpd
	for word, (w_host, w_port, w_database, w_user) in words:
		if (w_user == '*' or w_user == user) and \
			(w_host == '*' or w_host == host) and \
			(w_port == '*' or w_port == port) and \
		(w_database == '*' or w_database == database):
			return word

def lookup_password_file(path, t):
	'like lookup_password, but takes a file path'
	with open(path) as f:
		return lookup_password(parse(f), t)

def lookup_pgpass(d, passfile, exists = exists):
	# If the password file exists, lookup the password
	# using the config's criteria.
	if exists(passfile):
		return lookup_password_file(passfile, (
			str(d['user']), str(d['host']), str(d['port']),
			str(d.get('database', d['user']))
		))
