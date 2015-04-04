#!/usr/bin/env python
import sys
import os
from optparse import OptionParser
from .. import configfile
from .. import __version__

__all__ = ['command']

def command(args):
	"""
	pg_dotconf script entry point.
	"""
	op = OptionParser(
		"%prog [--stdout] [-f settings] postgresql.conf ([param=val]|[param])*",
		version = __version__
	)
	op.add_option(
		'-f', '--file',
		dest = 'settings',
		help = 'A file of settings to *apply* to the given "postgresql.conf"',
		default = [],
		action = 'append',
	)
	op.add_option(
		'--stdout',
		dest = 'stdout',
		help = 'Redirect the product to standard output instead of writing back to the "postgresql.conf" file',
		action = 'store_true',
		default = False
	)
	co, ca = op.parse_args(args[1:])
	if not ca:
		return 0

	settings = {}
	for sfp in co.settings:
		with open(sfp) as sf:
			for line in sf:
				pl = configfile.parse_line(line)
				if pl is not None:
					if comment not in line[pl[0].start]:
						settings[line[pl[0]]] = unquote(line[pl[1]])

	prev = None
	for p in ca[1:]:
		if '=' not in p:
			k = p
			v = None
		else:
			k, v = p.split('=', 1)
		k = k.strip()
		if not k:
			sys.stderr.write("ERROR: invalid setting, %r after %r%s" %(
				p, prev, os.linesep
			))
			sys.stderr.write(
				"HINT: Settings must take the form 'setting=value' " \
				"or 'setting_name_to_comment'. Settings must also be received " \
				"as a single argument." + os.linesep
			)
			sys.exit(1)
		prev = p
		settings[k] = v

	fp = ca[0]
	with open(fp, 'r') as fr:
		lines = configfile.alter_config(settings, fr)

	if co.stdout or fp == '/dev/stdin':
		for l in lines:
			sys.stdout.write(l)
	else:
		with open(fp, 'w') as fw:
			for l in lines:
				fw.write(l)
	return 0

if __name__ == '__main__':
	sys.exit(command(sys.argv))
