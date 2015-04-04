##
# .string
##
"""
String split and join operations for dealing with literals and identifiers.

Notably, the functions in this module are intended to be used for simple
use-cases. It attempts to stay away from "real" parsing and simply provides
functions for common needs, like the ability to identify unquoted portions of a
query string so that logic or transformations can be applied to only unquoted
portions. Scanning for statement terminators, or safely interpolating
identifiers.

All functions deal with strict quoting rules.
"""
import re

def escape_literal(text):
	"Replace every instance of ' with ''"
	return text.replace("'", "''")

def quote_literal(text):
	"Escape the literal and wrap it in [single] quotations"
	return "'" + text.replace("'", "''") + "'"

def escape_ident(text):
	'Replace every instance of " with ""'
	return text.replace('"', '""')

def needs_quoting(text):
	return not (text and not text[0].isdecimal() and text.replace('_', 'a').isalnum())

def quote_ident(text):
	"Replace every instance of '"' with '""' *and* place '"' on each end"
	return '"' + text.replace('"', '""') + '"'

def quote_ident_if_needed(text):
	"""
	If needed, replace every instance of '"' with '""' *and* place '"' on each end.
	Otherwise, just return the text.
	"""
	return quote_ident(text) if needs_quoting(text) else text

quote_re = re.compile(r"""(?xu)
	E'(?:''|\\.|[^'])*(?:'|$)          (?# Backslash escapes E'str')
|	'(?:''|[^'])*(?:'|$)               (?# Regular literals 'str')
|	"(?:""|[^"])*(?:"|$)               (?# Identifiers "str")
|	(\$(?:[^0-9$]\w*)?\$).*?(?:\1|$)   (?# Dollar quotes $$str$$)
""")

def split(text):
	"""
	split the string up by into non-quoted and quoted portions. Zero and even
	numbered indexes are unquoted portions, while odd indexes are quoted
	portions. 

	Unquoted portions are regular strings, whereas quoted portions are
	pair-tuples specifying the quotation mechanism and the content thereof.

	>>> list(split("select $$foobar$$"))
	['select ', ('$$', 'foobar'), '']

	If the split ends on a quoted section, it means the string's quote was not
	terminated. Subsequently, there will be an even number of objects in the
	list.

	Quotation errors are detected, but never raised. Rather it's up to the user
	to identify the best course of action for the given split.
	"""
	lastend = 0
	re = quote_re
	scan = re.scanner(text)
	match = scan.search()
	while match is not None:
		# text preceding the quotation
		yield text[lastend:match.start()]
		# the dollar quote, if any
		dq = match.groups()[0]
		if dq is not None:
			endoff = len(dq)
			quote = dq
			end = quote
		else:
			endoff = 1
			q = text[match.start()]
			if q == 'E':
				quote = "E'"
				end = "'"
			else:
				end = quote = q

		# If the end is not the expected quote, it consumed
		# the end. Be sure to check that the match's end - end offset
		# is *not* the start, ie an empty quotation at the end of the string.
		if text[match.end()-endoff:match.end()] != end \
		or match.end() - endoff == match.start():
			yield (quote, text[match.start()+len(quote):])
			break
		else:
			yield (quote, text[match.start()+len(quote):match.end()-endoff])

		lastend = match.end()
		match = scan.search()
	else:
		# balanced quotes, yield the rest
		yield text[lastend:]

def unsplit(splitted_iter):
	"""
	catenate a split string. This is needed to handle the special
	cases created by pg.string.split(). (Run-away quotations, primarily)
	"""
	s = ''
	quoted = False
	i = iter(splitted_iter)
	endq = ''
	for x in i:
		s += endq + x
		try:
			q, qtext = next(i)
			s += q + qtext
			if q == "E'":
				endq = "'"
			else:
				endq = q
		except StopIteration:
			break
	return s

def split_using(text, quote, sep = '.', maxsplit = -1):
	"""
	split the string on the seperator ignoring the separator in quoted areas.

	This is only useful for simple quoted strings. Dollar quotes, and backslash
	escapes are not supported.
	"""
	escape = quote * 2
	esclen = len(escape)
	offset = 0
	tl = len(text)
	end = tl
	# Fast path: No quotes? Do a simple split.
	if quote not in text:
		return text.split(sep, maxsplit)
	l = []

	while len(l) != maxsplit:
		# Look for the separator first
		nextsep = text.find(sep, offset)
		if nextsep == -1:
			# it's over. there are no more seps
			break
		else:
			# There's a sep ahead, but is there a quoted section before it?
			nextquote = text.find(quote, offset, nextsep)
			while nextquote != -1:
				# Yep, there's a quote before the sep;
				# need to eat the escaped portion.
				nextquote = text.find(quote, nextquote + 1,)
				while nextquote != -1:
					if text.find(escape, nextquote, nextquote+esclen) != nextquote:
						# Not an escape, so it's the end.
						break
					# Look for another quote past the escape quote.
					nextquote = text.find(quote, nextquote + 2)
				else:
					# the sep was located in the escape, and
					# the escape consumed the rest of the string.
					nextsep = -1
					break

				nextsep = text.find(sep, nextquote + 1)
				if nextsep == -1:
					# it's over. there are no more seps
					# [likely they were consumed by the escape]
					break
				nextquote = text.find(quote, nextquote + 1, nextsep)
			if nextsep == -1:
				break

			l.append(text[offset:nextsep])
			offset = nextsep + 1
	l.append(text[offset:])
	return l

def split_ident(text, sep = ',', quote = '"', maxsplit = -1):
	"""
	Split a series of identifiers using the specified separator.
	"""
	nr = []
	for x in split_using(text, quote, sep = sep, maxsplit = maxsplit):
		x = x.strip()
		if x.startswith('"'):
			if not x.endswith('"'):
				raise ValueError(
					"unterminated identifier quotation", x
				)
			else:
				nr.append(x[1:-1].replace('""', '"'))
		elif needs_quoting(x):
			raise ValueError(
				"non-ident characters in unquoted identifier", x
			)
		else:
			# postgres implies a lower, so to stay consistent
			# with it on qname joins, lower the unquoted identifier now.
			nr.append(x.lower())
	return nr

def split_qname(text, maxsplit = -1):
	"""
	Call to .split_ident() with a '.' sep parameter.
	"""
	return split_ident(text, maxsplit = maxsplit, sep = '.')

def qname(*args):
	"Quote the identifiers and join them using '.'"
	return '.'.join([quote_ident(x) for x in args])

def qname_if_needed(*args):
	return '.'.join([quote_ident_if_needed(x) for x in args])

def split_sql(sql, sep = ';'):
	"""
	Given SQL, safely split using the given separator.
	Notably, this yields fully split text. This should be used instead of
	split_sql_str() when quoted sections need be still be isolated.

	>>> list(split_sql('select $$1$$ AS "foo;"; select 2;'))
	[['select ', ('$$', '1'), ' AS ', ('"', 'foo;'), ''], (' select 2',), ['']]
	"""
	i = iter(split(sql))
	cur = []
	for part in i:
		sections = part.split(sep)

		if len(sections) < 2:
			cur.append(part)
		else:
			cur.append(sections[0])
			yield cur
			for x in sections[1:-1]:
				yield (x,)
			cur = [sections[-1]]
		try:
			cur.append(next(i))
		except StopIteration:
			break
	if cur:
		yield cur

def split_sql_str(sql, sep = ';'):
	"""
	Identical to split_sql but yields unsplit text.

	>>> list(split_sql_str('select $$1$$ AS "foo;"; select 2;'))
	['select $$1$$ AS "foo;"', ' select 2', '']
	"""
	for x in split_sql(sql, sep = sep):
		yield unsplit(x)
