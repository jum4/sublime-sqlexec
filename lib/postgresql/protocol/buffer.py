##
# .protocol.buffer
##
"""
This is an abstraction module that provides the working buffer implementation.
If a C compiler is not available on the system that built the package, the slower
`postgresql.protocol.pbuffer` module can be used in
`postgresql.port.optimized.buffer`'s absence.

This provides a convenient place to import the necessary module without
concerning the local code with the details.
"""
try:
	from ..port.optimized import pq_message_stream
except ImportError:
	from .pbuffer import pq_message_stream
