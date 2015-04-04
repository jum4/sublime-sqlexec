##
# .protocol.message_types
##
"""
Data module providing a sequence of bytes objects whose value corresponds to its
index in the sequence.

This provides resource for buffer objects to use common message type objects.

WARNING: It's tempting to use the 'is' operator and in some circumstances that
may be okay. However, it's possible (sys.modules.clear()) for the extension
modules' copy of this to become inconsistent with what protocol.element3 and
protocol.xact3 are using, so it's important to **not** use 'is'.
"""
message_types = tuple([bytes((x,)) for x in range(256)])
