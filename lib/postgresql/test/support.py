##
# .test.support
##
"""
Executable module used by test_* modules to mimic a command.
"""
import sys

def pg_config(*args):
	data = """FOO=BaR
FEH=YEAH
version=NAY
"""
	sys.stdout.write(data)

if __name__ == '__main__':
	if sys.argv[1:]:
		cmd = sys.argv[1]
		if cmd in globals():
			cmd = globals()[cmd]
			cmd(sys.argv[2:])
			sys.exit(0)
	sys.stderr.write("no valid entry point referenced")
	sys.exit(1)
