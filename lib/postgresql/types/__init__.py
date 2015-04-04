##
# types. - Package for I/O and PostgreSQL specific types.
##
"""
PostgreSQL types and identifiers.
"""
# XXX: Would be nicer to generate these from a header file...
InvalidOid = 0

RECORDOID = 2249
BOOLOID = 16
BITOID = 1560
VARBITOID = 1562
ACLITEMOID = 1033

CHAROID = 18
NAMEOID = 19
TEXTOID = 25
BYTEAOID = 17
BPCHAROID = 1042
VARCHAROID = 1043
CSTRINGOID = 2275
UNKNOWNOID = 705
REFCURSOROID = 1790
UUIDOID = 2950

TSVECTOROID = 3614
GTSVECTOROID = 3642
TSQUERYOID = 3615
REGCONFIGOID = 3734
REGDICTIONARYOID = 3769

JSONOID = 114
XMLOID = 142

MACADDROID = 829
INETOID = 869
CIDROID = 650

TYPEOID = 71
PROCOID = 81
CLASSOID = 83
ATTRIBUTEOID = 75

DATEOID = 1082
TIMEOID = 1083
TIMESTAMPOID = 1114
TIMESTAMPTZOID = 1184
INTERVALOID = 1186
TIMETZOID = 1266
ABSTIMEOID = 702
RELTIMEOID = 703
TINTERVALOID = 704

INT8OID = 20
INT2OID = 21
INT4OID = 23
OIDOID = 26
TIDOID = 27
XIDOID = 28
CIDOID = 29
CASHOID = 790
FLOAT4OID = 700
FLOAT8OID = 701
NUMERICOID = 1700

POINTOID = 600
LINEOID = 628
LSEGOID = 601
PATHOID = 602
BOXOID = 603
POLYGONOID = 604
CIRCLEOID = 718

OIDVECTOROID = 30
INT2VECTOROID = 22
INT4ARRAYOID = 1007

REGPROCOID = 24
REGPROCEDUREOID = 2202
REGOPEROID = 2203
REGOPERATOROID = 2204
REGCLASSOID = 2205
REGTYPEOID = 2206
REGTYPEARRAYOID = 2211

TRIGGEROID = 2279
LANGUAGE_HANDLEROID = 2280
INTERNALOID = 2281
OPAQUEOID = 2282
VOIDOID = 2278
ANYARRAYOID = 2277
ANYELEMENTOID = 2283
ANYOID = 2276
ANYNONARRAYOID = 2776
ANYENUMOID = 3500

#: Mapping of type Oid to SQL type name.
oid_to_sql_name = {
	BPCHAROID : 'CHARACTER',
	VARCHAROID : 'CHARACTER VARYING',
	# *OID : 'CHARACTER LARGE OBJECT',

	# SELECT X'0F' -> bit. XXX: Does bytea have any play here?
	#BITOID : 'BINARY',
	#BYTEAOID : 'BINARY VARYING',
	# *OID : 'BINARY LARGE OBJECT',

	BOOLOID : 'BOOLEAN',

# exact numeric types
	INT2OID : 'SMALLINT',
	INT4OID : 'INTEGER',
	INT8OID : 'BIGINT',
	NUMERICOID : 'NUMERIC',

# approximate numeric types
	FLOAT4OID : 'REAL',
	FLOAT8OID : 'DOUBLE PRECISION',

# datetime types
	TIMEOID : 'TIME WITHOUT TIME ZONE',
	TIMETZOID : 'TIME WITH TIME ZONE',
	TIMESTAMPOID : 'TIMESTAMP WITHOUT TIME ZONE',
	TIMESTAMPTZOID : 'TIMESTAMP WITH TIME ZONE',
	DATEOID : 'DATE',

# interval types
	INTERVALOID : 'INTERVAL',

	XMLOID : 'XML',
}

#: Mapping of type Oid to name.
oid_to_name = {
	RECORDOID : 'record',
	BOOLOID : 'bool',
	BITOID : 'bit',
	VARBITOID : 'varbit',
	ACLITEMOID : 'aclitem',

	CHAROID : 'char',
	NAMEOID : 'name',
	TEXTOID : 'text',
	BYTEAOID : 'bytea',
	BPCHAROID : 'bpchar',
	VARCHAROID : 'varchar',
	CSTRINGOID : 'cstring',
	UNKNOWNOID : 'unknown',
	REFCURSOROID : 'refcursor',
	UUIDOID : 'uuid',

	TSVECTOROID : 'tsvector',
	GTSVECTOROID : 'gtsvector',
	TSQUERYOID : 'tsquery',
	REGCONFIGOID : 'regconfig',
	REGDICTIONARYOID : 'regdictionary',

	XMLOID : 'xml',

	MACADDROID : 'macaddr',
	INETOID : 'inet',
	CIDROID : 'cidr',

	TYPEOID : 'type',
	PROCOID : 'proc',
	CLASSOID : 'class',
	ATTRIBUTEOID : 'attribute',

	DATEOID : 'date',
	TIMEOID : 'time',
	TIMESTAMPOID : 'timestamp',
	TIMESTAMPTZOID : 'timestamptz',
	INTERVALOID : 'interval',
	TIMETZOID : 'timetz',
	ABSTIMEOID : 'abstime',
	RELTIMEOID : 'reltime',
	TINTERVALOID : 'tinterval',

	INT8OID : 'int8',
	INT2OID : 'int2',
	INT4OID : 'int4',
	OIDOID : 'oid',
	TIDOID : 'tid',
	XIDOID : 'xid',
	CIDOID : 'cid',
	CASHOID : 'cash',
	FLOAT4OID : 'float4',
	FLOAT8OID : 'float8',
	NUMERICOID : 'numeric',

	POINTOID : 'point',
	LINEOID : 'line',
	LSEGOID : 'lseg',
	PATHOID : 'path',
	BOXOID : 'box',
	POLYGONOID : 'polygon',
	CIRCLEOID : 'circle',

	OIDVECTOROID : 'oidvector',
	INT2VECTOROID : 'int2vector',
	INT4ARRAYOID : 'int4array',

	REGPROCOID : 'regproc',
	REGPROCEDUREOID : 'regprocedure',
	REGOPEROID : 'regoper',
	REGOPERATOROID : 'regoperator',
	REGCLASSOID : 'regclass',
	REGTYPEOID : 'regtype',
	REGTYPEARRAYOID : 'regtypearray',

	TRIGGEROID : 'trigger',
	LANGUAGE_HANDLEROID : 'language_handler',
	INTERNALOID : 'internal',
	OPAQUEOID : 'opaque',
	VOIDOID : 'void',
	ANYARRAYOID : 'anyarray',
	ANYELEMENTOID : 'anyelement',
	ANYOID : 'any',
	ANYNONARRAYOID : 'anynonarray',
	ANYENUMOID : 'anyenum',
}

name_to_oid = dict(
	[(v,k) for k,v in oid_to_name.items()]
)

class Array(object):
	"""
	Type used to mimic PostgreSQL arrays. While there are many semantic
	differences, the primary one is that the elements contained by an Array
	instance are not strongly typed. The purpose of this class is to provide
	some consistency with PostgreSQL with respect to the structure of an Array.

	The structure consists of three parts:

	 * The elements of the array.
	 * The lower boundaries.
	 * The upper boundaries.

	There is also a `dimensions` property, but it is derived from the
	`lowerbounds` and `upperbounds` to yield a normalized description of the
	ARRAY's structure.

	The Python interfaces, such as __getitem__, are *not* subjected to the
	semantics of the lower and upper bounds. Rather, the normalized dimensions
	provide the primary influence for these interfaces. So, unlike SQL
	indirection, getting an index that does *not* exist will raise a Python
	`IndexError`.
	"""
	# return an iterator over the absolute elements of a nested sequence
	@classmethod
	def unroll_nest(typ, hier, dimensions, depth = 0):
		dsize = dimensions and dimensions[depth] or 0
		if len(hier) != dsize:
			raise ValueError("list size not consistent with dimensions at depth " + str(depth))
		r = []
		ndepth = depth + 1
		if ndepth == len(dimensions):
			# at the bottom
			r = hier
		else:
			# go deeper
			for x in hier:
				r.extend(typ.unroll_nest(x, dimensions, ndepth))
		return r

	# Detect the dimensions of a nested sequence
	@staticmethod
	def detect_dimensions(hier, len = len):
		# if the list is empty, it's a zero-dimension array.
		if hier:
			yield len(hier)
			hier = hier[0]
			depth = 1
			while hier.__class__ is list:
				depth += 1
				l = len(hier)
				if l < 1:
					raise ValueError("axis {0} is empty".format(depth))
				yield l
				hier = hier[0]

	@classmethod
	def from_elements(typ,
		elements : "iterable of elements in the array",
		lowerbounds : "beginning of each axis" = None,
		upperbounds : "upper bounds; size of each axis" = None,
		len = len,
	):
		"""
		Instantiate an Array from the given elements, lowerbounds, and upperbounds.

		The given elements are bound to the array which provides them with the
		structure defined by the lower boundaries and the upper boundaries.

		A `ValueError` will be raised in the following situations:

		 * The number of elements given are inconsistent with the number of elements
		   described by the upper and lower bounds.
		 * The lower bounds at a given axis exceeds the upper bounds at a given
		   axis.
		 * The number of lower bounds is inconsistent with the number of upper
		   bounds.
		"""
		# resolve iterable
		elements = list(elements)
		nelements = len(elements)

		# If ndims is zero, lowerbounds will be ()
		if lowerbounds is None:
			if upperbounds:
				lowerbounds = (1,) * len(upperbounds)
			elif nelements == 0:
				# special for empty ARRAY; no dimensions.
				lowerbounds = ()
			else:
				# one dimension.
				lowerbounds = (1,)
		else:
			lowerbounds = tuple(lowerbounds)

		if upperbounds is not None:
			upperbounds = tuple(upperbounds)
			dimensions = []
			# upperbounds were given, so check.
			if upperbounds:
				elcount = 1
				for lb, ub in zip(lowerbounds, upperbounds):
					x = ub - lb + 1
					if x < 1:
						# special case empty ARRAYs
						if nelements == 0:
							upperbounds = ()
							lowerbounds = ()
							dimensions = ()
							elcount = 0
							break
						raise ValueError("lowerbounds exceeds upperbounds")
					# physical dimensions.
					dimensions.append(x)
					elcount = x * elcount
			else:
				elcount = 0
			if nelements != elcount:
				raise ValueError("element count inconsistent with boundaries")
			dimensions = tuple(dimensions)
		else:
			# fill in default
			if nelements == 0:
				upperbounds = ()
				dimensions = ()
			else:
				upperbounds = (nelements,)
				dimensions = (nelements,)

		# consistency..
		if len(lowerbounds) != len(upperbounds):
			raise ValueError("number of lowerbounds inconsistent with upperbounds")

		rob = super().__new__(typ)
		rob._elements = elements
		rob.lowerbounds = lowerbounds
		rob.upperbounds = upperbounds
		rob.dimensions = dimensions
		rob.ndims = len(dimensions)
		rob._weight = len(rob._elements) // (dimensions and dimensions[0] or 1)
		return rob

	# Method used to create an Array() from nested lists.
	@classmethod
	def from_nest(typ, nest):
		dims = tuple(typ.detect_dimensions(nest))
		return typ.from_elements(
			list(typ.unroll_nest(nest, dims)),
			upperbounds = dims,
			# lowerbounds is implied to (1,)*len(upper)
		)

	def __new__(typ, nested_elements):
		"""
		Create an types.Array() using the given nested lists. The boundaries of
		the array are detected by traversing the first items of the nested
		lists::

			Array([[1,2,4],[3,4,8]])

		Lists are used to define the boundaries so that tuples may be used to
		represent any complex elements. The above array will the `lowerbounds`
		``(1,1)``, and the `upperbounds` ``(2,3)``.
		"""
		if nested_elements.__class__ is Array:
			return nested_elements
		return typ.from_nest(list(nested_elements))

	def __getnewargs__(self):
		return (self.nest(),)

	def elements(self):
		"""
		Returns an iterator to the elements of the Array. The elements are
		produced in physical order.
		"""
		return iter(self._elements)

	def nest(self, seqtype = list):
		"""
		Transform the array into a nested list.

		The `seqtype` keyword can be used to override the type used to represent
		the elements of a given axis.
		"""
		if self.ndims < 2:
			return seqtype(self._elements)
		else:
			rl = []
			for x in self:
				rl.append(x.nest(seqtype = seqtype))
			return seqtype(rl)

	def get_element(self, address,
		idxerr = "index {0} at axis {1} is out of range {2}".format
	):
		"""
		Get an element in the array using the given axis sequence.

		>>> a=Array([[1,2],[3,4]])
		>>> a.get_element((0,0)) == 1
		True
		>>> a.get_element((1,1)) == 4
		True

		This is similar to getting items in a nested list::

		>>> l=[[1,2],[3,4]]
		>>> l[0][0] == 1
		True
		"""
		if not self.dimensions:
			raise IndexError("array is empty")
		if len(address) != len(self.dimensions):
			raise ValueError("given axis sequence is inconsistent with number of dimensions")

		# normalize axis specification (-N + DIM), check for IndexErrors, and
		# resolve the element's position.
		cur = 0
		nelements = len(self._elements)
		for n, a, dim in zip(range(len(address)), address, self.dimensions):
			if a < 0:
				a = a + dim
				if a < 0:
					raise IndexError(idxerr(a, n, dim))
			else:
				if a >= dim:
					raise IndexError(idxerr(a, n, dim))
			nelements = nelements // dim
			cur += (a * nelements)
		return self._elements[cur]

	def sql_get_element(self, address):
		"""
		Like `get_element`, but with SQL indirection semantics. Notably, returns
		`None` on IndexError.
		"""
		try:
			a = [a - lb for (a, lb) in zip(address, self.lowerbounds)]
			# get_element accepts negatives, so check the converted sequence.
			for x in a:
				if x < 0:
					return None
			return self.get_element(a)
		except IndexError:
			return None

	def __repr__(self):
		return '%s.%s(%r)' %(
			type(self).__module__,
			type(self).__name__,
			self.nest()
		)

	def __len__(self):
		return self.dimensions and self.dimensions[0] or 0

	def __eq__(self, ob):
		return list(self) == ob

	def __ne__(self, ob):
		return list(self) != ob

	def __gt__(self, ob):
		return list(self) > ob

	def __lt__(self, ob):
		return list(self) < ob

	def __le__(self, ob):
		return list(self) <= ob

	def __ge__(self, ob):
		return list(self) >= ob

	def __getitem__(self, item):
		if self.ndims < 2:
			# Array with 1dim is more or less a list.
			return self._elements[item]
		if isinstance(item, slice):
			# get a sub-array slice
			l = len(self)
			n = 0
			r = []
			# for each offset in the slice, get the elements and add them
			# to the new elements list used to build the new Array().
			for x in range(*(item.indices(l))):
				n = n + 1
				r.extend(
					self._elements[slice(self._weight*x,self._weight*(x+1))]
				)
			if n:
				return self.__class__.from_elements(r,
					lowerbounds = (1,) + self.lowerbounds[1:],
					upperbounds = (n,) + self.upperbounds[1:],
				)
			else:
				# Empty
				return self.__class__.from_elements(())
		else:
			# get a sub-array
			l = len(self)
			if item > l:
				raise IndexError("index {0} is out of range".format(l))
			return self.__class__.from_elements(
				self._elements[self._weight*item:self._weight*(item+1)],
				lowerbounds = self.lowerbounds[1:],
				upperbounds = self.upperbounds[1:],
			)

	def __iter__(self):
		if self.ndims < 2:
			# Special case empty and single dimensional ARRAYs
			return self.elements()
		return (self[x] for x in range(len(self)))

from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
del itemgetter

class Row(tuple):
	"Name addressable items tuple; mapping and sequence"
	@classmethod
	def from_mapping(typ, keymap, map, get1 = get1):
		iter = [
			map.get(k) for k,_ in sorted(keymap.items(), key = get1)
		]
		r = typ(iter)
		r.keymap = keymap
		return r

	@classmethod
	def from_sequence(typ, keymap, seq):
		r = typ(seq)
		r.keymap = keymap
		return r

	def __getitem__(self, i, gi = tuple.__getitem__):
		if isinstance(i, (int, slice)):
			return gi(self, i)
		idx = self.keymap[i]
		return gi(self, idx)

	def get(self, i, gi = tuple.__getitem__, len = len):
		if type(i) is int:
			l = len(self)
			if -l < i < l:
				return gi(self, i)
		else:
			idx = self.keymap.get(i)
			if idx is not None:
				return gi(self, idx)
		return None

	def keys(self):
		return self.keymap.keys()

	def values(self):
		return iter(self)

	def items(self):
		return zip(iter(self.column_names), iter(self))

	def index_from_key(self, key):
		return self.keymap.get(key)

	def key_from_index(self, index):
		for k,v in self.keymap.items():
			if v == index:
				return k
		return None

	@property
	def column_names(self, get0 = get0, get1 = get1):
		l=list(self.keymap.items())
		l.sort(key=get1)
		return tuple(map(get0, l))

	def transform(self, *args, **kw):
		"""
		Make a new Row after processing the values with the callables associated
		with the values either by index, \*args, or my column name, \*\*kw.

			>>> r=Row.from_sequence({'col1':0,'col2':1}, (1,'two'))
			>>> r.transform(str)
			('1','two')
			>>> r.transform(col2 = str.upper)
			(1,'TWO')
			>>> r.transform(str, col2 = str.upper)
			('1','TWO')

		Combine with methodcaller and map to transform lots of rows:

			>>> rowseq = [r]
			>>> xf = operator.methodcaller('transform', col2 = str.upper)
			>>> list(map(xf, rowseq))
			[(1,'TWO')]

		"""
		r = list(self)
		i = 0
		for x in args:
			if x is not None:
				r[i] = x(tuple.__getitem__(self, i))
			i = i + 1
		for k,v in kw.items():
			if v is not None:
				i = self.index_from_key(k)
				if i is None:
					raise KeyError("row has no such key, " + repr(k))
				r[i] = v(self[k])
		return type(self).from_sequence(self.keymap, r)
