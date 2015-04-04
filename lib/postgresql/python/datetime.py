##
# python.datetime - parts needed to use stdlib.datetime
##
import datetime

##
# stdlib.datetime representation of PostgreSQL 'infinity' and '-infinity'.
infinity_datetime = datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999)
negative_infinity_datetime = datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0, 0)

infinity_date = datetime.date(datetime.MAXYEAR, 12, 31)
negative_infinity_date = datetime.date(datetime.MINYEAR, 1, 1)

class FixedOffset(datetime.tzinfo):
	def __init__(self, offset_in_seconds, tzname = None):
		self._tzname = tzname
		self._offset = offset_in_seconds
		self._offset_in_mins = offset_in_seconds // 60
		self._td_offset = datetime.timedelta(0, self._offset_in_mins * 60)
		self._dst = datetime.timedelta(0)

	def utcoffset(self, offset_from):
		return self._td_offset

	def tzname(self, dt):
		return self._tzname

	def dst(self, arg):
		return self._dst

	def __repr__(self):
		return "{path}.{name}({off}{tzname})".format(
			path = type(self).__module__,
			name = type(self).__name__,
			off = repr(self._td_offset.days * 24 * 60 * 60 + self._td_offset.seconds),
			tzname = (
				", tzname = {tzname!r}".format(tzname = self._tzname) \
				if self._tzname is not None else ""
			)
		)

UTC = FixedOffset(0, tzname = 'UTC')
