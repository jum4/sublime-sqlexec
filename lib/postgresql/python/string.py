##
# .python.string
##
import os

def indent(s, level = 2, char = ' '):
	ind = char * level
	r = ""
	for x in s.splitlines():
		r += ((ind + x).rstrip() + os.linesep)
	return r
