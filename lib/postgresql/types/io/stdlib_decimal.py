##
# types.io.stdlib_decimal
#
# I/O routines for transforming NUMERIC to and from decimal.Decimal.
##
from decimal import Decimal
from operator import itemgetter, mul
# You know it's gonna get serious :)
from itertools import chain, starmap, repeat, groupby, cycle, islice
from ...types import NUMERICOID
from . import lib

oid_to_type = {
	NUMERICOID: Decimal,
}

##
# numeric is represented using:
#  1. ndigits, the number of *numeric* digits.
#  2. weight, the *numeric* digits "left" of the decimal point
#  3. sign, negativity. see `numeric_signs` below
#  4. dscale, *display* precision. used to identify exponent.
#
# NOTE: A numeric digit is actually four digits in the representation.
#
# Python's Decimal consists of:
#  1. sign, negativity.
#  2. digits, sequence of int()'s
#  3. exponent, digits that fall to the right of the decimal point
numeric_negative = 16384

def numeric_pack(x,
	numeric_digit_length : "number of decimal digits in a numeric digit" = 4,
	get0 = itemgetter(0),
	get1 = itemgetter(1),
	Decimal = Decimal,
	pack = lib.numeric_pack
):
	if not isinstance(x, Decimal):
		x = Decimal(x)
	x = x.as_tuple()
	if x.exponent == 'F':
		raise ValueError("numeric does not support infinite values")

	# normalize trailing zeros (truncate em')
	# this is important in order to get the weight and padding correct
	# and to avoid packing superfluous data which will make pg angry.
	trailing_zeros = 0
	weight = 0
	if x.exponent < 0:
		# only attempt to truncate if there are digits after the point,
		##
		for i in range(-1, max(-len(x.digits), x.exponent)-1, -1):
			if x.digits[i] != 0:
				break
			trailing_zeros += 1
		# truncate trailing zeros right of the decimal point
		# this *is* the case as exponent < 0.
		if trailing_zeros:
			digits = x.digits[:-trailing_zeros]
		else:
			digits = x.digits
			# the entire exponent is just trailing zeros(zero-weight).
		rdigits = -(x.exponent + trailing_zeros)
		ldigits = len(digits) - rdigits
		rpad = rdigits % numeric_digit_length
		if rpad:
			rpad = numeric_digit_length - rpad
	else:
		# Need the weight to be divisible by four,
		# so append zeros onto digits until it is.
		r = (x.exponent % numeric_digit_length)
		if x.exponent and r:
			digits = x.digits + ((0,) * r)
			weight = (x.exponent - r)
		else:
			digits = x.digits
			weight = x.exponent
		# The exponent is not evenly divisible by four, so
		# the weight can't simple be x.exponent as it doesn't
		# match the size of the numeric digit.
		ldigits = len(digits)
		# no fractional quantity.
		rdigits = 0
		rpad = 0

	lpad = ldigits % numeric_digit_length
	if lpad:
		lpad = numeric_digit_length - lpad
	weight += (ldigits + lpad)

	digit_groups = map(
		get1,
		groupby(
			zip(
				# group by NUMERIC digit size,
				# every four digits make up a NUMERIC digit
				cycle((0,) * numeric_digit_length + (1,) * numeric_digit_length),

				# multiply each digit appropriately
				# for the eventual sum() into a NUMERIC digit
				starmap(
					mul,
					zip(
						# pad with leading zeros to make
						# the cardinality of the digit sequence
						# to be evenly divisible by four,
						# the NUMERIC digit size.
						chain(
							repeat(0, lpad),
							digits,
							repeat(0, rpad),
						),
						cycle([10**x for x in range(numeric_digit_length-1, -1, -1)]),
					)
				),
			),
			get0,
		),
	)
	return pack((
		(
			(ldigits + rdigits + lpad + rpad) // numeric_digit_length, # ndigits
			(weight // numeric_digit_length) - 1, # NUMERIC weight
			numeric_negative if x.sign == 1 else x.sign, # sign
			- x.exponent if x.exponent < 0 else 0, # dscale
		),
		list(map(sum, ([get1(y) for y in x] for x in digit_groups))),
	))

def numeric_convert_digits(d, str = str, int = int):
	i = iter(d)
	for x in str(next(i)):
		# no leading zeros
		yield int(x)
	# leading digit should not include zeros
	for y in i:
		for x in str(y).rjust(4, '0'):
			yield int(x)

numeric_signs = {
	numeric_negative : 1,
}

def numeric_unpack(x, unpack = lib.numeric_unpack):
	header, digits = unpack(x)
	npad = (header[3] - ((header[0] - (header[1] + 1)) * 4))
	return Decimal((
		numeric_signs.get(header[2], header[2]),
		tuple(chain(
			numeric_convert_digits(digits),
			(0,) * npad
		) if npad >= 0 else list(
			numeric_convert_digits(digits)
		)[:npad]),
		-header[3]
	))

oid_to_io = {
	NUMERICOID : (numeric_pack, numeric_unpack, Decimal),
}
