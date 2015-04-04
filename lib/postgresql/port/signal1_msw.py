##
# .port.signal1_msw
##
"""
Support for PG signals on Windows platforms.

This implementation supports all known versions of PostgreSQL. (2010)

CallNamedPipe:
	http://msdn.microsoft.com/en-us/library/aa365144%28VS.85%29.aspx
"""
import errno
from ctypes import windll, wintypes, pointer

# CallNamedPipe from kernel32.
CallNamedPipeA = windll.kernel32.CallNamedPipeA
CallNamedPipeA.restype = wintypes.BOOL
CallNamedPipeA.argtypes = (
	wintypes.LPCSTR, # in namedpipename
	wintypes.LPVOID, # in inbuffer (for signal number)
	wintypes.DWORD, # in inbuffersize (always 1)
	wintypes.LPVOID, # OutBuffer (signal return validation)
	wintypes.DWORD, # in OutBufferSize (always 1)
	wintypes.LPVOID, # out bytes read, really LPDWORD.
	wintypes.DWORD, # in timeout
)

from signal import SIGTERM, SIGINT, SIG_DFL
# SYNC: Values taken from the port/win32.h file.
SIG_DFL=0
SIGHUP=1
SIGQUIT=3
SIGTRAP=5
SIGABRT=22 # /* Set to match W32 value -- not UNIX value */
SIGKILL=9
SIGPIPE=13
SIGALRM=14
SIGSTOP=17
SIGTSTP=18
SIGCONT=19
SIGCHLD=20
SIGTTIN=21
SIGTTOU=22 # /* Same as SIGABRT -- no problem, I hope */
SIGWINCH=28
SIGUSR1=30
SIGUSR2=31

# SYNC: port.h
PG_SIGNAL_COUNT = 32

# In the situation of another variant, another module should be constructed.
def kill(pid : int, signal : int, timeout = 1000, dword1 = wintypes.DWORD(1)):
	"""
	Re-implementation of pg_kill for win32 using ctypes.
	"""
	if pid <= 0:
		raise OSError(errno.EINVAL, "process group not supported")
	if signal < 0 or signal >= PG_SIGNAL_COUNT:
		raise OSError(errno.EINVAL, "unsupported signal number")
	inbuffer = pointer(wintypes.BYTE(signal))
	outbuffer = pointer(wintypes.BYTE(0))
	outbytes = pointer(wintypes.DWORD(0))
	pidpipe = br'\\.\pipe\pgsignal_' + str(pid).encode('ascii')
	timeout = wintypes.DWORD(timeout)
	r = CallNamedPipeA(
		pidpipe, inbuffer, dword1, outbuffer, dword1, outbytes, timeout
	)
	if r:
		if outbuffer.contents.value == signal:
			if outbytes.contents.value == 1:
				# success
				return
	# Don't bother emulating the other failure cases/abstractions.
	# CallNamedPipeA should raise a WindowsError on those failures.
	raise OSError(errno.ESRCH, "unexpected output from CallNamedPipeA")
__docformat__ = 'reStructuredText'
