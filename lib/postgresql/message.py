##
# .message - PostgreSQL message representation
##
from operator import itemgetter
from .python.element import prime_factor
# Final msghook called exists at .sys.msghook
from . import sys as pg_sys

from .api import Message
class Message(Message):
	"""
	A message emitted by PostgreSQL. This element is universal, so
	`postgresql.api.Message` is a complete implementation for representing a
	message. Any interface should produce these objects.
	"""
	_e_label = property(lambda x: getattr(x, 'details').get('severity', 'MESSAGE'))
	_e_factors = ('creator',)

	def _e_metas(self, get0 = itemgetter(0)):
		yield (None, self.message)
		if self.code and self.code != "00000":
			yield ('CODE', self.code)
		locstr = self.location_string
		if locstr:
			yield ('LOCATION', locstr + ' from ' + self.source)
		else:
			yield ('LOCATION', self.source)
		for k, v in sorted(self.details.items(), key = get0):
			if k not in self.standard_detail_coverage:
				yield (k.upper(), str(v))

	source = 'SERVER'
	code = '00000'
	message = None
	details = None

	severities = (
		'DEBUG',
		'INFO',
		'NOTICE',
		'WARNING',
		'ERROR',
		'FATAL',
		'PANIC',
	)
	sources = (
		'SERVER',
		'CLIENT',
	)

	def isconsistent(self, other):
		"""
		Return `True` if the all the fields of the message in `self` are
		equivalent to the fields in `other`.
		"""
		if not isinstance(other, self.__class__):
			return False
		# creator is contextual information
		return (
			self.code == other.code and \
			self.message == other.message and \
			self.details == other.details and \
			self.source == other.source
		)

	def __init__(self,
		message : "The primary information of the message",
		code : "Message code to attach (SQL state)" = None,
		details : "additional information associated with the message" = {},
		source : "Which side generated the message(SERVER, CLIENT)" = None,
		creator : "The interface element that called for instantiation" = None,
	):
		self.message = message
		self.details = details
		self.creator = creator
		if code is not None and self.code != code:
			self.code = code
		if source is not None and self.source != source:
			self.source = source

	def __repr__(self):
		return "{mod}.{typname}({message!r}{code}{details}{source}{creator})".format(
			mod = self.__module__,
			typname = self.__class__.__name__,
			message = self.message,
			code = (
				"" if self.code == type(self).code
				else ", code = " + repr(self.code)
			),
			details = (
				"" if not self.details
				else ", details = " + repr(self.details)
			),
			source = (
				"" if self.source is None
				else ", source = " + repr(self.source)
			),
			creator = (
				"" if self.creator is None
				else ", creator = " + repr(self.creator)
			)
		)

	@property
	def location_string(self):
		"""
		A single line representation of the 'file', 'line', and 'function' keys
		in the `details` dictionary.
		"""
		details = self.details
		loc = [
			details.get(k, '?') for k in ('file', 'line', 'function')
		]
		return (
			"" if loc == ['?', '?', '?']
			else "File {0!r}, "\
			"line {1!s}, in {2!s}".format(*loc)
		)

	# keys to filter in .details
	standard_detail_coverage = frozenset(['message', 'severity', 'file', 'function', 'line',])

	def emit(self, starting_point = None):
		"""
		Take the given message object and hand it to all the primary
		factors(creator) with a msghook callable.
		"""
		if starting_point is not None:
			f = starting_point
		else:
			f = self.creator

		while f is not None:
			if getattr(f, 'msghook', None) is not None:
				if f.msghook(self):
					# the trap returned a nonzero value,
					# so don't continue raising. (like with's __exit__)
					return f
			f = prime_factor(f)
			if f:
				f = f[1]
		# if the next primary factor is without a raise or does not exist,
		# send the message to postgresql.sys.msghook
		pg_sys.msghook(self)
