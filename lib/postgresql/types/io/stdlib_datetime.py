##
# stdlib_datetime - support for the stdlib's datetime.
#
# I/O routines for date, time, timetz, timestamp, timestamptz, and interval.
# Supported by the datetime module.
##
import datetime
import warnings
from functools import partial
from operator import methodcaller, add

from ...python.datetime import UTC, FixedOffset, \
	infinity_date, infinity_datetime, \
	negative_infinity_date, negative_infinity_datetime
from ...python.functools import Composition as compose
from ...exceptions import TypeConversionWarning

from .. import \
	DATEOID, INTERVALOID, \
	TIMEOID, TIMETZOID, \
	TIMESTAMPOID, TIMESTAMPTZOID

from . import lib

oid_to_type = {
	DATEOID: datetime.date,
	TIMESTAMPOID: datetime.datetime,
	TIMESTAMPTZOID: datetime.datetime,
	TIMEOID: datetime.time,
	TIMETZOID: datetime.time,

	# XXX: datetime.timedelta doesn't support months.
	INTERVALOID: datetime.timedelta,
}

seconds_in_day = 24 * 60 * 60
seconds_in_hour = 60 * 60

pg_epoch_datetime = datetime.datetime(2000, 1, 1)
pg_epoch_datetime_utc = pg_epoch_datetime.replace(tzinfo = UTC)
pg_epoch_date = pg_epoch_datetime.date()
pg_date_offset = pg_epoch_date.toordinal()

## Difference between PostgreSQL epoch and Unix epoch.
## Used to convert a PostgreSQL ordinal to an ordinal usable by datetime
pg_time_days = (pg_date_offset - datetime.date(1970, 1, 1).toordinal())

##
# Constants used to special case infinity and -infinity.
time64_pack_constants = {
	infinity_datetime: lib.time64_infinity,
	negative_infinity_datetime: lib.time64_negative_infinity,
	'infinity': lib.time64_infinity,
	'-infinity': lib.time64_negative_infinity,
}
time_pack_constants = {
	infinity_datetime: lib.time_infinity,
	negative_infinity_datetime: lib.time_negative_infinity,
	'infinity': lib.time_infinity,
	'-infinity': lib.time_negative_infinity,
}
date_pack_constants = {
	infinity_date: lib.date_infinity,
	negative_infinity_date: lib.date_negative_infinity,
	'infinity': lib.date_infinity,
	'-infinity': lib.date_negative_infinity,
}
time64_unpack_constants = {
	lib.time64_infinity: infinity_datetime,
	lib.time64_negative_infinity: negative_infinity_datetime,
}
time_unpack_constants = {
	lib.time_infinity: infinity_datetime,
	lib.time_negative_infinity: negative_infinity_datetime,
}
date_unpack_constants = {
	lib.date_infinity: infinity_date,
	lib.date_negative_infinity: negative_infinity_date,
}

def date_pack(x,
	pack = lib.date_pack,
	offset = pg_date_offset,
	get = date_pack_constants.get,
):
	return get(x) or pack(x.toordinal() - offset)

def date_unpack(x,
	unpack = lib.date_unpack,
	offset = pg_date_offset,
	from_ord = datetime.date.fromordinal,
	get = date_unpack_constants.get,
):
	return get(x) or from_ord(unpack(x) + pg_date_offset)

def timestamp_pack(x,
	seconds_in_day = seconds_in_day,
	pg_epoch_datetime = pg_epoch_datetime,
):
	"""
	Create a (seconds, microseconds) pair from a `datetime.datetime` instance.
	"""
	x = (x - pg_epoch_datetime)
	return ((x.days * seconds_in_day) + x.seconds, x.microseconds)

def timestamp_unpack(seconds,
	timedelta = datetime.timedelta,
	relative_to = pg_epoch_datetime.__add__,
):
	"""
	Create a `datetime.datetime` instance from a (seconds, microseconds) pair.
	"""
	return relative_to(timedelta(0, *seconds))

def timestamptz_pack(x,
	seconds_in_day = seconds_in_day,
	pg_epoch_datetime_utc = pg_epoch_datetime_utc,
	UTC = UTC,
):
	"""
	Create a (seconds, microseconds) pair from a `datetime.datetime` instance.
	"""
	x = (x.astimezone(UTC) - pg_epoch_datetime_utc)
	return ((x.days * seconds_in_day) + x.seconds, x.microseconds)

def timestamptz_unpack(seconds,
	timedelta = datetime.timedelta,
	relative_to = pg_epoch_datetime_utc.__add__,
):
	"""
	Create a `datetime.datetime` instance from a (seconds, microseconds) pair.
	"""
	return relative_to(timedelta(0, *seconds))

def time_pack(x, seconds_in_hour = seconds_in_hour):
	"""
	Create a (seconds, microseconds) pair from a `datetime.time` instance.
	"""
	return (
		(x.hour * seconds_in_hour) + (x.minute * 60) + x.second,
		x.microsecond
	)

def time_unpack(seconds_ms, time = datetime.time, divmod = divmod):
	"""
	Create a `datetime.time` instance from a (seconds, microseconds) pair.
	Seconds being offset from epoch.
	"""
	seconds, ms = seconds_ms
	minutes, sec = divmod(seconds, 60)
	hours, min = divmod(minutes, 60)
	return time(hours, min, sec, ms)

def interval_pack(x):
	"""
	Create a (months, days, (seconds, microseconds)) tuple from a
	`datetime.timedelta` instance.
	"""
	return (0, x.days, (x.seconds, x.microseconds))

def interval_unpack(mds, timedelta = datetime.timedelta):
	"""
	Given a (months, days, (seconds, microseconds)) tuple, create a
	`datetime.timedelta` instance.
	"""
	months, days, seconds_ms = mds
	if months != 0:
		# XXX: Should this raise an exception?
		w = TypeConversionWarning(
			"datetime.timedelta cannot represent relative intervals",
			details = {
				'hint': 'An interval was unpacked with a non-zero "month" field.'
			},
			source = 'DRIVER'
		)
		warnings.warn(w)
	return timedelta(
		days = days + (months * 30),
		seconds = seconds_ms[0], microseconds = seconds_ms[1]
	)

def timetz_pack(x,
	time_pack = time_pack,
):
	"""
	Create a ((seconds, microseconds), timezone) tuple from a `datetime.time`
	instance.
	"""
	td = x.tzinfo.utcoffset(x)
	seconds = (td.days * seconds_in_day + td.seconds)
	return (time_pack(x), seconds)

def timetz_unpack(tstz,
	time_unpack = time_unpack,
	FixedOffset = FixedOffset,
):
	"""
	Create a `datetime.time` instance from a ((seconds, microseconds), timezone)
	tuple.
	"""
	t = time_unpack(tstz[0])
	return t.replace(tzinfo = FixedOffset(tstz[1]))

FloatTimes = False
IntTimes = True
NoDay = True
WithDay = False

# Used to handle the special cases: infinity and -infinity.
def proc_when_not_in(proc, dict):
	def _proc(x, get=dict.get):
		return get(x) or proc(x)
	return _proc

id_to_io = {
	(FloatTimes, TIMEOID) : (
		compose((time_pack, lib.time_pack)),
		compose((lib.time_unpack, time_unpack)),
		datetime.time
	),
	(FloatTimes, TIMETZOID) : (
		compose((timetz_pack, lib.timetz_pack)),
		compose((lib.timetz_unpack, timetz_unpack)),
		datetime.time
	),
	(FloatTimes, TIMESTAMPOID) : (
		proc_when_not_in(compose((timestamp_pack, lib.time_pack)), time_pack_constants),
		proc_when_not_in(compose((lib.time_unpack, timestamp_unpack)), time_unpack_constants),
		datetime.datetime
	),
	(FloatTimes, TIMESTAMPTZOID) : (
		proc_when_not_in(compose((timestamptz_pack, lib.time_pack)), time_pack_constants),
		proc_when_not_in(compose((lib.time_unpack, timestamptz_unpack)), time_unpack_constants),
		datetime.datetime
	),
	(FloatTimes, WithDay, INTERVALOID): (
		compose((interval_pack, lib.interval_pack)),
		compose((lib.interval_unpack, interval_unpack)),
		datetime.timedelta
	),
	(FloatTimes, NoDay, INTERVALOID): (
		compose((interval_pack, lib.interval_noday_pack)),
		compose((lib.interval_noday_unpack, interval_unpack)),
		datetime.timedelta
	),

	(IntTimes, TIMEOID) : (
		compose((time_pack, lib.time64_pack)),
		compose((lib.time64_unpack, time_unpack)),
		datetime.time
	),
	(IntTimes, TIMETZOID) : (
		compose((timetz_pack, lib.timetz64_pack)),
		compose((lib.timetz64_unpack, timetz_unpack)),
		datetime.time
	),
	(IntTimes, TIMESTAMPOID) : (
		proc_when_not_in(compose((timestamp_pack, lib.time64_pack)), time64_pack_constants),
		proc_when_not_in(compose((lib.time64_unpack, timestamp_unpack)), time64_unpack_constants),
		datetime.datetime
	),
	(IntTimes, TIMESTAMPTZOID) : (
		proc_when_not_in(compose((timestamptz_pack, lib.time64_pack)), time64_pack_constants),
		proc_when_not_in(compose((lib.time64_unpack, timestamptz_unpack)), time64_unpack_constants),
		datetime.datetime
	),
	(IntTimes, WithDay, INTERVALOID) : (
		compose((interval_pack, lib.interval64_pack)),
		compose((lib.interval64_unpack, interval_unpack)),
		datetime.timedelta
	),
	(IntTimes, NoDay, INTERVALOID) : (
		compose((interval_pack, lib.interval64_noday_pack)),
		compose((lib.interval64_noday_unpack, interval_unpack)),
		datetime.timedelta
	),
}

##
# Identify whether it's IntTimes or FloatTimes
def time_type(typio):
	idt = typio.database.settings.get('integer_datetimes', None)
	if idt is None:
		# assume its absence means its on after 9.0
		return bool(typio.database.version_info >= (9,0))
	elif idt.__class__ is bool:
		return idt
	else:
		return (idt.lower() in ('on', 'true', 't', True))

def select_format(oid, typio, get = id_to_io.__getitem__):
	return get((time_type(typio), oid))

def select_day_format(oid, typio, get = id_to_io.__getitem__):
	return get((time_type(typio), typio.database.version_info[:2] <= (8,0), oid))

oid_to_io = {
	DATEOID : (date_pack, date_unpack, datetime.date,),
	TIMEOID : select_format,
	TIMETZOID : select_format,
	TIMESTAMPOID : select_format,
	TIMESTAMPTZOID : select_format,
	INTERVALOID : select_day_format,
}
