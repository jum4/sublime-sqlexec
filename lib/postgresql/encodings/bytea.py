##
# .encodings.bytea
##
'PostgreSQL bytea encoding and decoding functions'
import codecs
import struct
import sys

ord_to_seq = {
	i : \
		"\\" + oct(i)[2:].rjust(3, '0') \
		if not (32 < i < 126) else r'\\' \
		if i == 92 else chr(i)
	for i in range(256)
}

if sys.version_info[:2] >= (3, 3):
	# Subscripting memory in 3.3 returns byte as an integer, not as a bytestring
	def decode(data):
		return ''.join(map(ord_to_seq.__getitem__, (data[x] for x in range(len(data)))))
else:
	def decode(data):
		return ''.join(map(ord_to_seq.__getitem__, (data[x][0] for x in range(len(data)))))

def encode(data):
	diter = ((data[i] for i in range(len(data))))
	output = []
	next = diter.__next__
	for x in diter:
		if x == "\\":
			try:
				y = next()
			except StopIteration:
				raise ValueError("incomplete backslash sequence")
			if y == "\\":
				# It's a backslash, so let x(\) be appended.
				x = ord(x)
			elif y.isdigit():
				try:
					os = ''.join((y, next(), next()))
				except StopIteration:
					# requires three digits
					raise ValueError("incomplete backslash sequence")
				try:
					x = int(os, base = 8)
				except ValueError:
					raise ValueError("invalid bytea octal sequence '%s'" %(os,))
			else:
				raise ValueError("invalid backslash follow '%s'" %(y,))
		else:
			x = ord(x)
		output.append(x)
	return struct.pack(str(len(output)) + 'B', *output)

class Codec(codecs.Codec):
	'bytea codec'
	def encode(data, errors = 'strict'):
		return (encode(data), len(data))
	encode = staticmethod(encode)

	def decode(data, errors = 'strict'):
		return (decode(data), len(data))
	decode = staticmethod(decode)

class StreamWriter(Codec, codecs.StreamWriter): pass
class StreamReader(Codec, codecs.StreamReader): pass

bytea_codec = (Codec.encode, Codec.decode, StreamReader, StreamWriter)
codecs.register(lambda x: x == 'bytea' and bytea_codec or None)
