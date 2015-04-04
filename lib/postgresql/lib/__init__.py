##
# .lib - libraries; manage SQL outside of Python.
##
"""
PostgreSQL statement and object libraries.

The purpose of a library is provide a means to manage a mapping of symbols
to database operations or objects. These operations can be simple statements,
procedures, or something more complex.

Libraries are intended to allow the programmer to isolate and manage SQL outside
of a system's code-flow. It provides a means to construct the basic Python
interfaces to a PostgreSQL-based application.
"""
import io
import os.path
from types import ModuleType
from abc import abstractmethod, abstractproperty
from ..python.element import Element, ElementSet
from .. import api as pg_api
from .. import sys as pg_sys
from .. import exceptions as pg_exc
from ..python.itertools import find
from itertools import chain

try:
	libdir = os.path.abspath(os.path.dirname(__file__))
except NameError:
	pass
else:
	if os.path.exists(libdir):
		pg_sys.libpath.insert(0, libdir)
	del libdir

__all__ = [
	'Library',
	'SymbolCollection',
	'ILF',
	'Symbol',
	'Binding',
	'BoundSymbol',
	'find_libsql',
	'load',
]

class Symbol(Element):
	"""
	An annotated SQL statement string.

	The annotations describe how the statement should be used.
	"""
	__slots__ = (
		'library',
		'source',
		'name',
		'method',
		'type',
		'parameters',
	)
	_e_label = 'SYMBOL'
	_e_factors = ('library', 'source',)

	# The statement execution methods; symbols allow this to be specified
	# in order for a default method to be selected.
	execution_methods = {
		'first',
		'rows',
		'chunks',
		'declare',
		'load_chunks',
		'load_rows',
		'column',
	}

	def _e_metas(self):
		yield (None, self.name)

	def __init__(self,
		library, source,
		name = None,
		method = None,
		type = None,
		parameters = None,
		reference = False,
	):
		self.library = library
		self.source = source
		self.name = name
		if method in (None, '', 'all'):
			method = None
		elif method not in self.execution_methods:
			raise ValueError("unknown execution method: " + repr(method))
		self.method = method
		self.type = type
		self.parameters = parameters
		self.reference = reference

	def __str__(self):
		"""
		Provide the source of the query's symbol.
		"""
		# Explicitly run str() on source as it is expected that a
		# given symbol's source may be generated.
		return str(self.source)

class Library(Element):
	"""
	A library is mapping of symbol names to `postgresql.lib.Symbol` instances.
	"""
	_e_label = 'LIBRARY'
	_e_factors = ()

	@abstractproperty
	def address(self) -> str:
		"""
		A string indicating the source of the symbols.
		"""

	@abstractproperty
	def name(self) -> str:
		"""
		The name to bind the library as. Should be an identifier.
		"""

	@abstractproperty
	def preload(self) -> {str,}:
		"""
		A set of symbols that should prepared when the library is bound.
		"""

	@abstractmethod
	def symbols(self) -> [str]:
		"""
		Iterable of symbol names provides by the library.
		"""

	@abstractmethod
	def get_symbol(self, name) -> (Symbol, [Symbol]):
		"""
		Return the symbol with the given name.
		"""

class SymbolCollection(Library):
	"""
	Explicitly composed library. (Symbols passed into __init__)
	"""
	preload = None
	symtypes = (
		'static',
		'preload',
		'const',
		'proc',
		'transient',
	)

	def __init__(self, symbols, preface = None):
		"""
		Given an iterable of (symtype, symexe, doc, sql) tuples, create a
		symbol collection.
		"""
		self.preface = preface
		self._address = None
		self._name = None
		s = self.symbolsd = {}
		self.preload = set()
		for name, (isref, typ, exe, doc, query) in symbols:
			if typ and typ not in self.symtypes:
				raise ValueError(
					"symbol %r has an invalid type: %r" %(name, typ)
				)
			if typ == 'preload':
				self.preload.add(name)
				typ = None
			elif typ == 'proc':
				pass
			SYM = Symbol(self, query,
				name = name,
				method = exe,
				type = typ,
				reference = isref
			)
			s[name] = SYM

class ILF(SymbolCollection):
	'INI Library Format'
	def _e_metas(self):
		yield (None, self._address or 'ILF')

	def __repr__(self):
		return self.__class__.__module__ + '.' + self.__class__.__name__ + '.open(' + repr(self.address) + ')'

	@property
	def name(self):
		return self._name

	@property
	def address(self):
		return self._address

	def get_symbol(self, name):
		return self.symbolsd.get(name)

	def symbols(self):
		return self.symbolsd.keys()

	@classmethod
	def from_lines(typ, lines):
		"""
		Create an anonymous ILF library from a sequence of lines.
		"""
		prev = ''
		curid = None
		curblock = []
		blocks = []
		for line in lines:
			l = line.strip()
			if l.startswith('[') and l.endswith(']'):
				blocks.append((curid, curblock))
				curid = line
				curblock = []
			elif line.startswith('*[') and ']' in line:
				ref, rest = line.split(']', 1)
				# strip the leading '*['
				ref = ref[2:]
				# dereferencing will take place later.
				curblock.append((ref, rest))
			else:
				curblock.append(line)
		blocks.append((curid, curblock))
		preface = ''.join(blocks.pop(0)[1])
		syms = []
		for symdesc, block in blocks:
			# symbol name
			# symbol type
			# how to execute symbol
			name, styp, exe, *_ = (tuple(
				symdesc.strip().strip('[]').split(':')
			) + (None, None))
			doc = ''
			endofcomment = 0
			# resolve any symbol references; only one per line.
			block = [
				x if x.__class__ is not tuple else (
					find(reversed(syms), lambda y: y[0] == x[0])[1][-1] + x[1]
				)
				for x in block
			]
			for x in block:
				if x.startswith('-- '):
					doc += x[3:]
				else:
					break
				endofcomment += 1
			query = ''.join(block[endofcomment:])
			if styp == 'proc':
				query = query.strip()
			if name.startswith('&'):
				name = name[1:]
				isref = True
			else:
				isref = False
			syms.append((name, (isref, styp, exe, doc, query)))
		return typ(syms, preface = preface)

	@classmethod
	def open(typ, filepath, *args, **kw):
		"""
		Create a named ILF library from a file path.
		"""
		with io.open(filepath, *args, **kw) as fp:
			r = typ.from_lines(fp)
			r._address = os.path.abspath(filepath)
			bn = os.path.basename(filepath)
			if bn.startswith('lib') and bn.endswith('.sql'):
				r._name = bn[3:-4] or None
		return r

class BoundSymbol(object):
	"""
	A symbol bound to a database(connection).
	"""
	def __init__(self, symbol, database):
		if symbol.type == 'proc':
			proc = database.proc(symbol)
			self.method = proc.__call__
			self.object = proc
		else:
			ps = database.prepare(symbol)
			m = symbol.method
			if m is None:
				self.method = ps.__call__
			else:
				self.method = getattr(ps, m)
			self.object = ps

	def __call__(self, *args, **kw):
		return self.method(*args, **kw)

class BoundReference(object):
	"""
	A symbol bound to a database whose results make up the source of a symbol
	that will be created upon the execution of this symbol.

	A reference to a symbol.
	"""

	def __init__(self, symbol, database):
		self.symbol = symbol
		self.database = database
		self.method = database.prepare(symbol).chunks

	def __call__(self, *args, **kw):
		chunks = chain.from_iterable(self.method(*args, **kw))
		# Join columns with a space, and rows with a newline.
		src = '\n'.join([' '.join(row) for row in chunks])
		return BoundSymbol(
			Symbol(
				self.symbol.library, src,
				name = self.symbol.name,
				method = self.symbol.method,
				type = self.symbol.type,
				parameters = self.symbol.parameters,
				reference = False,
			),
			self.database,
		)

class Binding(object):
	"""
	Library bound to a database(connection).
	"""
	def __init__(self, database, library):
		self.__dict__.update({
			'__database__' : database,
			'__symbol_library__' : library,
			'__symbol_cache__' : {},
		})
		for x in library.preload:
			# cache all preloaded symbols.
			getattr(self, x)

	def __repr__(self):
		return '<Binding: lib%s on %r>' %(
			self.__symbol_library__.name,
			self.__database__
		)

	def __dir__(self):
		return dir(super()) + list(self.__symbol_library__.symbols())

	def __getattr__(self, name):
		"""
		Return a BoundSymbol against the Binding's database with the
		symbol named ``name`` in the Binding's library.
		"""
		d = self.__dict__
		s = d['__symbol_cache__']
		db = d['__database__']
		lib = d['__symbol_library__']

		bs = s.get(name)
		if bs is None:
			# No symbol cached with that name.
			# Everything is crammed in here because
			# we do *not* want methods on this object.
			# The namespace is primarily reserved for symbols.
			sym = lib.get_symbol(name)
			if sym is None:
				raise AttributeError(
					"symbol %r does not exist in library %r" %(
						name, lib.address
					)
				)
			if sym.reference:
				# Reference.
				bs = BoundReference(sym, db)
				if sym.type == 'const':
					# Constant Reference means a BoundSymbol.
					bs = bs()
				if sym.type != 'transient':
					s[name] = bs
			else:
				if not isinstance(sym, Symbol):
					# subjective symbol...
					sym = sym(db)
					if not isinstance(sym, Symbol):
						raise TypeError(
							"callable symbol, %r, did not produce " \
							"Symbol instance" %(name,)
						)
				if sym.type == 'const':
					r = BoundSymbol(sym, db)()
					if sym.method in ('chunks', 'rows', 'column'):
						# resolve the iterator
						r = list(r)
					bs = s[name] = r
				else:
					bs = BoundSymbol(sym, db)
					if sym.type != 'transient':
						s[name] = bs
		return bs

class Category(pg_api.Category):
	"""
	Library-based Category.
	"""
	_e_factors = ('libraries',)
	def _e_metas(self):
		yield ('aliases', {k.name: v for k, v in self.aliases.items()})

	def __init__(self, *libs, **named_libs):
		sl = set(libs)
		nl = set(named_libs.values())
		self._direct = sl
		self.libraries = ElementSet(sl | nl)
		self.aliases = {}
		# lib -> [alias-1, alias-2, ..., alias-n]
		for k, v in named_libs.items():
			d = self.aliases.setdefault(v, [])
			d.append(k)

	def __call__(self, database):
		for l in self.libraries:
			names = list(self.aliases.get(l, ()))
			if l in self._direct:
				names.append(l.name)
			B = Binding(database, l)
			for n in names:
				if hasattr(database, n):
					raise AttributeError("attribute already exists: " + name)
				setattr(database, n, B)

def find_libsql(libname, paths, prefix = 'lib', suffix = '.sql'):
	"""
	Given the base library name, `libname`, look for a file named
	"<prefix><libname><suffix>" in each directory(`paths`).
	All finds will be yielded out.
	"""
	lib = prefix + libname + suffix
	for p in paths:
		p = os.path.join(p, lib)
		if os.path.exists(p):
			yield p

def load(libref):
	"""
	Given a reference to a symbol library, instantiate the Library instance.

	Currently this function accepts:

	 * `str` objects as absolute paths or relative to sys.libpath.
	 * Module objects.
	"""
	if isinstance(libref, ModuleType):
		if hasattr(libref, '__lib'):
			lib = getattr(libref, '__lib')
		else:
			lib = ModuleLibrary(libref)
			setattr(libref, '__lib', lib)
	elif isinstance(libref, str):
		try:
			if os.path.sep in libref:
				# sep in libref? it's treated as a path.
				lib = ILF.open(libref)
			else:
				# first one wins.
				for x in find_libsql(libref, pg_sys.libpath):
					break
				else:
					raise pg_exc.LoadError("library %r not in postgresql.sys.libpath" % (libref,))
				lib = ILF.open(x)
		except pg_exc.LoadError:
			raise
		except Exception:
			# any exception is a load error.
			raise pg_exc.LoadError("failed load ILF, " + repr(libref))
	else:
		raise TypeError("load takes a module or str, given " + type(libref).__name__)
	return lib

sys = load('sys')

__docformat__ = 'reStructuredText'
