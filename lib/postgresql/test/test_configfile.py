##
# .test.test_configfile
##
import os
import unittest
from io import StringIO
from .. import configfile

sample_config_Aroma = \
"""
##
# A sample config file.
##
# This provides a good = test for alter_config.

#shared_buffers = 4500
search_path = window,$user,public
shared_buffers = 2500

port = 5234
listen_addresses = 'localhost'
listen_addresses = '*'
"""

##
# Wining cases are alteration cases that provide
# source and expectations from an alteration.
#
# The first string is the source, the second the
# alterations to make, the and the third, the expectation.
##
winning_cases = [
	(
		# Two top contenders; the first should be altered, second commented.
		"foo = bar"+os.linesep+"foo = bar",
		{'foo' : 'newbar'},
		"foo = 'newbar'"+os.linesep+"#foo = bar"
	),
	(
		# Two top contenders, first one stays commented
		"#foo = bar"+os.linesep+"foo = bar",
		{'foo' : 'newbar'},
		"#foo = bar"+os.linesep+"foo = 'newbar'"
	),
	(
		# Two top contenders, second one stays commented
		"foo = bar"+os.linesep+"#foo = bar",
		{'foo' : 'newbar'},
		"foo = 'newbar'"+os.linesep+"#foo = bar"
	),
	(
		# Two candidates
		"foo = bar"+os.linesep+"foo = none",
		{'foo' : 'bar'},
		"foo = 'bar'"+os.linesep+"#foo = none"
	),
	(
		# Two candidates, winner should be the first, second gets comment
		"#foo = none"+os.linesep+"foo = bar",
		{'foo' : 'none'},
		"foo = 'none'"+os.linesep+"#foo = bar"
	),
	(
		# Two commented candidates
		"#foo = none"+os.linesep+"#foo = some",
		{'foo' : 'bar'},
		"foo = 'bar'"+os.linesep+"#foo = some"
	),
	(
		# Two commented candidates, the latter a top contender
		"#foo = none"+os.linesep+"#foo = bar",
		{'foo' : 'bar'},
		"#foo = none"+os.linesep+"foo = 'bar'"
	),
	(
		# Replace empty value
		"foo = "+os.linesep,
		{'foo' : 'feh'},
		"foo = 'feh'"
	),
	(
		# Comment value
		"foo = bar",
		{'foo' : None},
		"#foo = bar"
	),
	(
		# Commenting after value
		"foo = val this should be commented",
		{'foo' : 'newval'},
		"foo = 'newval' #this should be commented"
	),
	(
		# Commenting after value
		"#foo = val this should be commented",
		{'foo' : 'newval'},
		"foo = 'newval' #this should be commented"
	),
	(
		# Commenting after quoted value
		"#foo = 'val'foo this should be commented",
		{'foo' : 'newval'},
		"foo = 'newval' #this should be commented"
	),
	(
		# Adjacent post-value comment
		"#foo = 'val'#foo this should be commented",
		{'foo' : 'newval'},
		"foo = 'newval'#foo this should be commented"
	),
	(
		# New setting in empty string
		"",
		{'bar' : 'newvar'},
		"bar = 'newvar'",
	),
	(
		# New setting
		"foo = 'bar'",
		{'bar' : 'newvar'},
		"foo = 'bar'"+os.linesep+"bar = 'newvar'",
	),
	(
		# New setting with quote escape
		"foo = 'bar'",
		{'bar' : "new'var"},
		"foo = 'bar'"+os.linesep+"bar = 'new''var'",
	),
]

class test_configfile(unittest.TestCase):
	def parseNone(self, line):
		sl = configfile.parse_line(line)
		if sl is not None:
			self.fail(
				"With line %r, parsed out to %r, %r, and %r, %r, " \
				"but expected None to be returned by parse function." %(
					line, line[sl[0]], sl[0], line[sl[0]], sl[0]
				)
			)

	def parseExpect(self, line, key, val):
		line = line %(key, val)
		sl = configfile.parse_line(line)
		if sl is None:
			self.fail(
				"expecting %r and %r from line %r, " \
				"but got None(syntax error) instead." %(
					key, val, line
				)
			)
		k, v = sl
		if line[k] != key:
			self.fail(
				"expecting key %r for line %r, " \
				"but got %r from %r instead." %(
					key, line, line[k], k
				)
			)
		if line[v] != val:
			self.fail(
				"expecting value %r for line %r, " \
				"but got %r from %r instead." %(
					val, line, line[v], v
				)
			)

	def testParser(self):
		self.parseExpect("#%s = %s", 'foo', 'none')
		self.parseExpect("#%s=%s"+os.linesep, 'foo', 'bar')
		self.parseExpect(" #%s=%s"+os.linesep, 'foo', 'bar')
		self.parseExpect('%s =%s'+os.linesep, 'foo', 'bar')
		self.parseExpect(' %s=%s '+os.linesep, 'foo', 'Bar')
		self.parseExpect(' %s = %s '+os.linesep, 'foo', 'Bar')
		self.parseExpect('# %s = %s '+os.linesep, 'foo', 'Bar')
		self.parseExpect('\t # %s = %s '+os.linesep, 'foo', 'Bar')
		self.parseExpect('  # %s =   %s '+os.linesep, 'foo', 'Bar')
		self.parseExpect("  # %s = %s"+os.linesep, 'foo', "' Bar '")
		self.parseExpect("%s = %s# comment"+os.linesep, 'foo', '')
		self.parseExpect("  # %s = %s # A # comment"+os.linesep, 'foo', "' B''a#r '")
		# No equality or equality in complex comment
		self.parseNone(' #i  # foo =   Bar '+os.linesep)
		self.parseNone('#bar')
		self.parseNone('bar')

	def testConfigRead(self):
		sample = "foo = bar"+os.linesep+"# A comment, yes."+os.linesep+" bar = foo # yet?"+os.linesep
		d = configfile.read_config(sample.split(os.linesep))
		self.assertTrue(d['foo'] == 'bar')
		self.assertTrue(d['bar'] == 'foo')

	def testConfigWriteRead(self):
		strio = StringIO()
		d = {
			'' : "'foo bar'"
		}
		configfile.write_config(d, strio.write)
		strio.seek(0)

	def testWinningCases(self):
		i = 0
		for before, alters, after in winning_cases:
			befg = (x + os.linesep for x in before.split(os.linesep))
			became = ''.join(configfile.alter_config(alters, befg))
			self.assertTrue(
				became.strip() == after,
				'On %d, before, %r, did not become after, %r; got %r using %r' %(
					i, before, after, became, alters
				)
			)
			i += 1

	def testSimpleConfigAlter(self):
		# Simple set and uncomment and set test.
		strio = StringIO()
		strio.write("foo = bar"+os.linesep+" # bleh = unset"+os.linesep+" # grr = 'oh yeah''s'")
		strio.seek(0)
		lines = configfile.alter_config({'foo' : 'yes', 'bleh' : 'feh'}, strio)
		d = configfile.read_config(lines)
		self.assertTrue(d['foo'] == 'yes')
		self.assertTrue(d['bleh'] == 'feh')
		self.assertTrue(''.join(lines).count('bleh') == 1)

	def testAroma(self):
		lines = configfile.alter_config({
				'shared_buffers' : '800',
				'port' : None
			}, (x + os.linesep for x in sample_config_Aroma.split('\n'))
		)
		d = configfile.read_config(lines)
		self.assertTrue(d['shared_buffers'] == '800')
		self.assertTrue(d.get('port') is None)

		nlines = configfile.alter_config({'port' : '1'}, lines)
		d2 = configfile.read_config(nlines)
		self.assertTrue(d2.get('port') == '1')
		self.assertTrue(
			nlines[:4] == lines[:4]
		)
	
	def testSelection(self):
		# Sanity
		red = configfile.read_config(['foo = bar'+os.linesep, 'bar = foo'])
		self.assertTrue(len(red.keys()) == 2)

		# Test a simple selector
		red = configfile.read_config(['foo = bar'+os.linesep, 'bar = foo'],
			selector = lambda x: x == 'bar')
		rkeys = list(red.keys())
		self.assertTrue(len(rkeys) == 1)
		self.assertTrue(rkeys[0] == 'bar')
		self.assertTrue(red['bar'] == 'foo')

if __name__ == '__main__':
	unittest.main()
