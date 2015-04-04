##
# .encodings.aliases
##
"""
Module for mapping PostgreSQL encoding names to Python encoding names.

These are **not** installed in Python's aliases. Rather, `get_python_name`
should be used directly.

URLs of interest:
 * http://docs.python.org/library/codecs.html
 * http://git.postgresql.org/gitweb?p=postgresql.git;a=blob;f=src/backend/utils/mb/encnames.c
"""

##
#: Dictionary of Postgres encoding names to Python encoding names.
#: This mapping only contains those encoding names that do not intersect.
postgres_to_python = {
	'unicode' : 'utf_8',
	'sql_ascii' : 'ascii',
	'euc_jp' : 'eucjp',
	'euc_cn' : 'euccn',
	'euc_kr' : 'euckr',
	'shift_jis_2004' : 'euc_jis_2004',
	'sjis' : 'shift_jis',
	'alt' : 'cp866', # IBM866
	'abc' : 'cp1258',
	'vscii' : 'cp1258',
	'koi8r' : 'koi8_r',
	'koi8u' : 'koi8_u',
	'tcvn' : 'cp1258',
	'tcvn5712' : 'cp1258',
#	'euc_tw' : None, # N/A
#	'mule_internal' : None, # N/A
}

def get_python_name(encname):
	"""
	Lookup the name in the `postgres_to_python` dictionary. If no match is
	found, check for a 'win' or 'windows-' name and convert that to a 'cp###'
	name.

	Returns `None` if there is no alias for `encname`.

	The win[0-9]+ and windows-[0-9]+ entries are handled functionally.
	"""
	# check the dictionary first
	localname = postgres_to_python.get(encname)
	if localname is not None:
		return localname
	# no explicit mapping, check for functional transformation
	if encname.startswith('win'):
		# handle win#### and windows-####
		# remove the trailing CP number
		bare = encname.rstrip('0123456789')
		if bare.strip('_-') in ('win', 'windows'):
			return 'cp' + encname[len(bare):]
	return encname
