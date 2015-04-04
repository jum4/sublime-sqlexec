##
# .test.test_string
##
import sys
import os
import unittest
from .. import string as pg_str

# strange possibility, split, normalized
split_qname_samples = [
	('base', ['base'], 'base'),
	('bASe', ['base'], 'base'),
	('"base"', ['base'], 'base'),
	('"base "', ['base '], '"base "'),
	('" base"', [' base'], '" base"'),
	('" base"""', [' base"'], '" base"""'),
	('""" base"""', ['" base"'], '""" base"""'),
	('".base"', ['.base'], '".base"'),
	('".base."', ['.base.'], '".base."'),
	('schema.base', ['schema', 'base'], 'schema.base'),
	('"schema".base', ['schema', 'base'], 'schema.base'),
	('schema."base"', ['schema', 'base'], 'schema.base'),
	('"schema.base"', ['schema.base'], '"schema.base"'),
	('schEmÅ."base"', ['schemå', 'base'], 'schemå.base'),
	('scheMa."base"', ['schema', 'base'], 'schema.base'),
	('sche_ma.base', ['sche_ma', 'base'], 'sche_ma.base'),
	('_schema.base', ['_schema', 'base'], '_schema.base'),
	('a000.b111', ['a000', 'b111'], 'a000.b111'),
	('" schema"."base"', [' schema', 'base'], '" schema".base'),
	('" schema"."ba se"', [' schema', 'ba se'], '" schema"."ba se"'),
	('" ""schema"."ba""se"', [' "schema', 'ba"se'], '" ""schema"."ba""se"'),
	('" schema" . "ba se"', [' schema', 'ba se'], '" schema"."ba se"'),
	(' " schema" . "ba se"	', [' schema', 'ba se'], '" schema"."ba se"'),
	(' ". schema." . "ba se"	', ['. schema.', 'ba se'], '". schema."."ba se"'),
	('CAT . ". schema." . "ba se"	', ['cat', '. schema.', 'ba se'],
		'cat.". schema."."ba se"'),
	('"cat" . ". schema." . "ba se"	', ['cat', '. schema.', 'ba se'],
		'cat.". schema."."ba se"'),
	('"""cat" . ". schema." . "ba se"	', ['"cat', '. schema.', 'ba se'],
		'"""cat".". schema."."ba se"'),
	('"""cÅt" . ". schema." . "ba se"	', ['"cÅt', '. schema.', 'ba se'],
		'"""cÅt".". schema."."ba se"'),
]

split_samples = [
	('', ['']),
	('one-to-one', ['one-to-one']),
	('"one-to-one"', [
		'',
		('"', 'one-to-one'),
		''
	]),
	('$$one-to-one$$', [
		'',
		('$$', 'one-to-one'),
		''
	]),
	("E'one-to-one'", [
		'',
		("E'", 'one-to-one'),
		''
	]),
	("E'on''e-to-one'", [
		'',
		("E'", "on''e-to-one"),
		''
	]),
	("E'on''e-to-\\'one'", [
		'',
		("E'", "on''e-to-\\'one"),
		''
	]),
	("'one\\'-to-one'", [
		'',
		("'", "one\\"),
		"-to-one",
		("'", ''),
	]),

	('"foo"""', [
		'',
		('"', 'foo""'),
		'',
	]),

	('"""foo"', [
		'',
		('"', '""foo'),
		'',
	]),

	("'''foo'", [
		'',
		("'", "''foo"),
		'',
	]),
	("'foo'''", [
		'',
		("'", "foo''"),
		'',
	]),
	("E'foo\\''", [
		'',
		("E'", "foo\\'"),
		'',
	]),
	(r"E'foo\\' '", [
		'',
		("E'", r"foo\\"),
		' ',
		("'", ''),
	]),
	(r"E'foo\\'' '", [
		'',
		("E'", r"foo\\'' "),
		'',
	]),

	('select \'foo\' as "one"', [
		'select ',
		("'", 'foo'),
		' as ',
		('"', 'one'),
		''
	]),
	('select $$foo$$ as "one"', [
		'select ',
		("$$", 'foo'),
		' as ',
		('"', 'one'),
		''
	]),
	('select $b$foo$b$ as "one"', [
		'select ',
		("$b$", 'foo'),
		' as ',
		('"', 'one'),
		''
	]),
	('select $b$', [
		'select ',
		('$b$', ''),
	]),

	('select $1', [
		'select $1',
	]),

	('select $1$', [
		'select $1$',
	]),
]

split_sql_samples = [
	('select 1; select 1', [
		['select 1'],
		[' select 1']
	]),
	('select \'one\' as "text"; select 1', [
		['select ', ("'", 'one'), ' as ', ('"', 'text'), ''],
		[' select 1']
	]),
	('select \'one\' as "text"; select 1', [
		['select ', ("'", 'one'), ' as ', ('"', 'text'), ''],
		[' select 1']
	]),
	('select \'one;\' as ";text;"; select 1; foo', [
		['select ', ("'", 'one;'), ' as ', ('"', ';text;'), ''],
		(' select 1',),
		[' foo'],
	]),
	('select \'one;\' as ";text;"; select $$;$$; foo', [
		['select ', ("'", 'one;'), ' as ', ('"', ';text;'), ''],
		[' select ', ('$$', ';'), ''],
		[' foo'],
	]),
	('select \'one;\' as ";text;"; select $$;$$; foo;\';b\'\'ar\'', [
		['select ', ("'", 'one;'), ' as ', ('"', ';text;'), ''],
		[' select ', ('$$', ';'), ''],
		(' foo',),
		['', ("'", ";b''ar"), ''],
	]),
]

class test_strings(unittest.TestCase):
	def test_split(self):
		for unsplit, split in split_samples:
			xsplit = list(pg_str.split(unsplit))
			self.assertEqual(xsplit, split)
			self.assertEqual(pg_str.unsplit(xsplit), unsplit)
	
	def test_split_sql(self):
		for unsplit, split in split_sql_samples:
			xsplit = list(pg_str.split_sql(unsplit))
			self.assertEqual(xsplit, split)
			self.assertEqual(';'.join([pg_str.unsplit(x) for x in xsplit]), unsplit)

	def test_qname(self):
		"indirectly tests split_using"
		for unsplit, split, norm in split_qname_samples:
			xsplit = pg_str.split_qname(unsplit)
			self.assertEqual(xsplit, split)
			self.assertEqual(pg_str.qname_if_needed(*split), norm)

		self.assertRaises(
			ValueError,
			pg_str.split_qname, '"foo'
		)
		self.assertRaises(
			ValueError,
			pg_str.split_qname, 'foo"'
		)
		self.assertRaises(
			ValueError,
			pg_str.split_qname, 'bar.foo"'
		)
		self.assertRaises(
			ValueError,
			pg_str.split_qname, 'bar".foo"'
		)
		self.assertRaises(
			ValueError,
			pg_str.split_qname, '0bar.foo'
		)
		self.assertRaises(
			ValueError,
			pg_str.split_qname, 'bar.fo@'
		)

	def test_quotes(self):
		self.assertEqual(
			pg_str.quote_literal("""foo'bar"""),
			"""'foo''bar'"""
		)
		self.assertEqual(
			pg_str.quote_literal("""\\foo'bar\\"""),
			"""'\\foo''bar\\'"""
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed("foo"),
			"foo"
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed("0foo"),
			'"0foo"'
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed("foo0"),
			'foo0'
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed("_"),
			'_'
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed("_9"),
			'_9'
		)
		self.assertEqual(
			pg_str.quote_ident_if_needed('''\\foo'bar\\'''),
			'''"\\foo'bar\\"'''
		)
		self.assertEqual(
			pg_str.quote_ident("spam"),
			'"spam"'
		)
		self.assertEqual(
			pg_str.qname("spam", "ham"),
			'"spam"."ham"'
		)
		self.assertEqual(
			pg_str.escape_ident('"'),
			'""',
		)
		self.assertEqual(
			pg_str.escape_ident('""'),
			'""""',
		)
		chars = ''.join([
			chr(x) for x in range(10000)
			if chr(x) != '"'
		])
		self.assertEqual(
			pg_str.escape_ident(chars),
			chars,
		)
		chars = ''.join([
			chr(x) for x in range(10000)
			if chr(x) != "'"
		])
		self.assertEqual(
			pg_str.escape_literal(chars),
			chars,
		)
		chars = ''.join([
			chr(x) for x in range(10000)
			if chr(x) not in "\\'"
		])
		self.assertEqual(
			pg_str.escape_literal(chars),
			chars,
		)

if __name__ == '__main__':
	from types import ModuleType
	this = ModuleType("this")
	this.__dict__.update(globals())
	unittest.main(this)
