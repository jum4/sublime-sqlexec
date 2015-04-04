class Varbit(object):
	__slots__ = ('data', 'bits')

	def from_bits(subtype, bits, data):
		if bits == 1:
			return (data[0] & (1 << 7)) and OneBit or ZeroBit
		else:
			rob = object.__new__(subtype)
			rob.bits = bits
			rob.data = data
			return rob
	from_bits = classmethod(from_bits)

	def __new__(typ, data):
		if isinstance(data, Varbit):
			return data
		if isinstance(data, bytes):
			return typ.from_bits(len(data) * 8, data)
		# str(), eg '00101100'
		bits = len(data)
		nbytes, remain = divmod(bits, 8)
		bdata = [bytes((int(data[x:x+8], 2),)) for x in range(0, bits - remain, 8)]
		if remain != 0:
			bdata.append(bytes((int(data[nbytes*8:].ljust(8,'0'), 2),)))
		return typ.from_bits(bits, b''.join(bdata))

	def __str__(self):
		if self.bits:
			# cut off the remainder from the bits
			blocks = [bin(x)[2:].rjust(8, '0') for x in self.data]
			blocks[-1] = blocks[-1][0:(self.bits % 8) or 8]
			return ''.join(blocks)
		else:
			return ''

	def __repr__(self):
		return '%s.%s(%r)' %(
			type(self).__module__,
			type(self).__name__,
			str(self)
		)

	def __eq__(self, ob):
		if not isinstance(ob, type(self)):
			ob = type(self)(ob)
		return ob.bits == self.bits and ob.data == self.data

	def __len__(self):
		return self.bits

	def __add__(self, ob):
		return Varbit(str(self) + str(ob))

	def __mul__(self, ob):
		return Varbit(str(self) * ob)

	def getbit(self, bitoffset):
		if bitoffset < 0:
			idx = self.bits + bitoffset
		else:
			idx = bitoffset
		if not 0 <= idx < self.bits:
			raise IndexError("bit index %d out of range" %(bitoffset,))

		byte, bitofbyte = divmod(idx, 8)
		if ord(self.data[byte]) & (1 << (7 - bitofbyte)):
			return OneBit
		else:
			return ZeroBit

	def __getitem__(self, item):
		if isinstance(item, slice):
			return type(self)(str(self)[item])
		else:
			return self.getbit(item)

	def __nonzero__(self):
		for x in self.data:
			if x != 0:
				return True
		return False

class Bit(Varbit):
	def __new__(subtype, ob):
		if ob is ZeroBit or ob is False or ob == '0':
			return ZeroBit
		elif ob is OneBit or ob is True or ob == '1':
			return OneBit

		raise ValueError('unknown bit value %r, 0 or 1' %(ob,))

	def __nonzero__(self):
		return self is OneBit

	def __str__(self):
		return self is OneBit and '1' or '0'

ZeroBit = object.__new__(Bit)
ZeroBit.data = b'\x00'
ZeroBit.bits = 1
OneBit = object.__new__(Bit)
OneBit.data = b'\x80'
OneBit.bits = 1
