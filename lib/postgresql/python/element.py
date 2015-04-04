##
# .python.element
##
import os
from abc import ABCMeta, abstractproperty, abstractmethod
from .string import indent
from .decorlib import propertydoc

class RecursiveFactor(Exception):
	'Raised when a factor is ultimately composed of itself'
	pass

class Element(object, metaclass = ABCMeta):
	"""
	The purpose of an element is to provide a general mechanism for specifying
	the factors that composed an object. Factors are designated using an
	ordered set of strings referencing those significant attributes on the object.

	Factors are important for PG-API as it provides the foundation for
	collecting the information about the state of the interface that ultimately
	led up to an error.

		Traceback:
		 ...
		postgresql.exceptions.*: <message>
		  CODE: XX000
		CURSOR: <cursor_id>
		  parameters: (p1, p2, ...)
		STATEMENT: <statement_id> <parameter info>
		  ...
		  string:
		    <query body>
		  SYMBOL: get_types
		  LIBRARY: catalog
		  ...
		CONNECTION:
		  <backend_id> <socket information>
		CONNECTOR: [Host]
		  IRI: pq://user@localhost:5432/database
		DRIVER: postgresql.driver.pq3
	"""

	@propertydoc
	@abstractproperty
	def _e_label(self) -> str:
		"""
		Single-word string describing the kind of element.

		For instance, `postgresql.api.Statement`'s _e_label is 'STATEMENT'.

		Usually, this is set directly on the class itself, and is a shorter
		version of the class's name.
		"""

	@propertydoc
	@abstractproperty
	def _e_factors(self) -> ():
		"""
		The attribute names of the objects that contributed to the creation of
		this object.

		The ordering is significant. The first factor is the prime factor.
		"""

	@abstractmethod
	def _e_metas(self) -> [(str, object)]:
		"""
		Return an iterable to key-value pairs that provide useful descriptive
		information about an attribute.

		Factors on metas are not checked. They are expected to be primitives.

		If there are no metas, the str() of the object will be used to represent
		it.
		"""

class ElementSet(Element, set):
	"""
	An ElementSet is a set of Elements that can be used as an individual factor.

	In situations where a single factor is composed of multiple elements where
	each has no significance over the other, this Element can be used represent
	that fact.

	Importantly, it provides the set metadata so that the appropriate information
	will be produced in element tracebacks.
	"""
	_e_label = 'SET'
	_e_factors = ()
	__slots__ = ()

	def _e_metas(self):
		yield (None, len(self))
		for x in self:
			yield (None, '--')
			yield (None, format_element(x))

def prime_factor(obj):
	'get the primary factor on the `obj`, returns None if none.'
	f = getattr(obj, '_e_factors', None)
	if f:
		return f[0], getattr(obj, f[0], None)

def prime_factors(obj):
	"""
	Yield out the sequence of primary factors of the given object.
	"""
	visited = set((obj,))
	ef = getattr(obj, '_e_factors', None)
	if not ef:
		return
	fn = ef[0]
	e = getattr(obj, fn, None)
	if e in visited:
		raise RecursiveFactor(obj, e)
	visited.add(e)
	yield fn, e

	while e is not None:
		ef = getattr(obj, '_e_factors', None)
		fn = ef[0]
		e = getattr(e, fn, None)
		if e in visited:
			raise RecursiveFactor(obj, e)
		visited.add(e)
		yield fn, e

def format_element(obj, coverage = ()):
	'format the given element with its factors and metadata into a readable string'
	# if it's not an Element, all there is to return is str(obj)
	if obj in coverage:
		raise RecursiveFactor(coverage)
	coverage = coverage + (obj,)

	if not isinstance(obj, Element):
		if obj is None:
			return 'None'
		return str(obj)

	# The description of `obj` is built first.

	# formal element, get metas first.
	nolead = False
	metas = []
	for key, val in obj._e_metas():
		m = ""
		if val is None:
			sval = 'None'
		else:
			sval = str(val)

		pre = ' '
		if key is not None:
			m += key + ':'
			if (len(sval) > 70 or os.linesep in sval):
				pre = os.linesep
				sval = indent(sval)
		else:
			# if the key is None, it is intended to be inlined.
			nolead = True
			pre = ''
		m += pre + sval.rstrip()
		metas.append(m)

	factors = []
	for att in obj._e_factors[1:]:
		m = ""
		f = getattr(obj, att)
		# if the object has a label, use the label
		m += att + ':'
		sval = format_element(f, coverage = coverage)
		if len(sval) > 70 or os.linesep in sval:
			m += os.linesep + indent(sval)
		else:
			m += ' ' + sval
		factors.append(m)

	mtxt = os.linesep.join(metas)
	ftxt = os.linesep.join(factors)
	if mtxt:
		mtxt = indent(mtxt)
	if ftxt:
		ftxt = indent(ftxt)
	s = mtxt + ftxt
	if nolead is True:
		# metas started with a `None` key.
		s = ' ' + s.lstrip()
	else:
		s = os.linesep + s
	s = obj._e_label + ':' + s.rstrip()

	# and resolve the next prime
	pf = prime_factor(obj)
	if pf is not None:
		factor_name, prime = pf
		factor = format_element(prime, coverage = coverage)
		if getattr(prime, '_e_label', None) is not None:
			# if the factor has a label, then it will be
			# included in the format_element output, and
			# thus factor_name is not needed.
			factor_name = ''
		else:
			factor_name += ':'
			if len(factor) > 70 or os.linesep in factor:
				factor = os.linesep + indent(factor)
			else:
				factor_name += ' '
		s += os.linesep + factor_name + factor
	return s
