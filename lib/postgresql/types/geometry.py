import math
from operator import itemgetter
get0 = itemgetter(0)
get1 = itemgetter(1)
# Geometric types

class Point(tuple):
	"""
	A point; a pair of floating point numbers.
	"""
	__slots__ = ()
	x = property(fget = lambda s: s[0])
	y = property(fget = lambda s: s[1])

	def __new__(subtype, pair):
		return tuple.__new__(subtype, (float(pair[0]), float(pair[1])))

	def __repr__(self):
		return '%s.%s(%s)' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self),
		)

	def __str__(self):
		return tuple.__repr__(self)

	def __add__(self, ob):
		wx, wy = ob
		return type(self)((self[0] + wx, self[1] + wy))

	def __sub__(self, ob):
		wx, wy = ob
		return type(self)((self[0] - wx, self[1] - wy))

	def __mul__(self, ob):
		wx, wy = ob
		rx = (self[0] * wx) - (self[1] * wy)
		ry = (self[0] * wy) + (self[1] * wx)
		return type(self)((rx, ry))

	def __div__(self, ob):
		sx, sy = self
		wx, wy = ob
		div = (wx * wx) + (wy * wy)
		rx = ((sx * wx) + (sy * wy)) / div
		ry = ((wx * sy) + (wy * sx)) / div
		return type(self)((rx, ry))

	def distance(self, ob, sqrt = math.sqrt):
		wx, wy = ob
		dx = self[0] - float(wx)
		dy = self[1] - float(wy)
		return sqrt(dx**2 + dy**2)

class Lseg(tuple):
	__slots__ = ()
	one = property(fget = lambda s: s[0])
	two = property(fget = lambda s: s[1])

	length = property(fget = lambda s: s[0].distance(s[1]))
	vertical = property(fget = lambda s: s[0][0] == s[1][0])
	horizontal = property(fget = lambda s: s[0][1] == s[1][1])
	slope = property(
		fget = lambda s: (s[1][1] - s[0][1]) / (s[1][0] - s[0][0])
	)
	center = property(
		fget = lambda s: Point((
			(s[0][0] + s[1][0]) / 2.0,
			(s[0][1] + s[1][1]) / 2.0,
		))
	)

	def __new__(subtype, pair):
		p1, p2 = pair
		return tuple.__new__(subtype, (Point(p1), Point(p2)))

	def __repr__(self):
		# Avoid the point representation
		return '%s.%s(%s, %s)' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			tuple.__repr__(self[1]),
		)

	def __str__(self):
		return '[(%s,%s),(%s,%s)]' %(
			self[0][0],
			self[0][1],
			self[1][0],
			self[1][1],
		)

	def parallel(self, ob):
		return self.slope == type(self)(ob).slope

	def intersect(self, ob):
		raise NotImplementedError

	def perpendicular(self, ob):
		return (self.slope / type(self)(ob).slope) == -1.0

class Box(tuple):
	"""
	A pair of points. One specifying the top-right point of the box; the other
	specifying the bottom-left. `high` being top-right; `low` being bottom-left.

	http://www.postgresql.org/docs/current/static/datatype-geometric.html

		>>> Box(( (0,0), (-2, -2) ))
		postgresql.types.geometry.Box(((0.0, 0.0), (-2.0, -2.0)))

	It will also relocate values to enforce the high-low expectation:

		>>> t.box(((-4,0),(-2,-3)))
		postgresql.types.geometry.Box(((-2.0, 0.0), (-4.0, -3.0)))

	::
		
		                (-2, 0) `high`
		                   |
		                   |
		    (-4,-3) -------+-x
		     `low`         y

	This happens because ``-4`` is less than ``-2``; therefore the ``-4``
	belongs on the low point. This is consistent with what PostgreSQL does
	with its ``box`` type.
	"""
	__slots__ = ()
	high = property(fget = get0, doc = "high point of the box")
	low = property(fget = get1, doc = "low point of the box")
	center = property(
		fget = lambda s: Point((
			(s[0][0] + s[1][0]) / 2.0,
			(s[0][1] + s[1][1]) / 2.0
		)),
		doc = "center of the box as a point"
	)

	def __new__(subtype, hl):
		if isinstance(hl, Box):
			return hl
		one, two = hl
		if one[0] > two[0]:
			hx = one[0]
			lx = two[0]
		else:
			hx = two[0]
			lx = one[0]
		if one[1] > two[1]:
			hy = one[1]
			ly = two[1]
		else:
			hy = two[1]
			ly = one[1]
		return tuple.__new__(subtype, (Point((hx, hy)), Point((lx, ly))))

	def __repr__(self):
		return '%s.%s((%s, %s))' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			tuple.__repr__(self[1]),
		)

	def __str__(self):
		return '%s,%s' %(self[0], self[1])

class Circle(tuple):
	"""
	type for PostgreSQL circles
	"""
	__slots__ = ()
	center = property(fget = get0, doc = "center of the circle (point)")
	radius = property(fget = get1, doc = "radius of the circle (radius >= 0)")

	def __new__(subtype, pair):
		center, radius = pair
		if radius < 0:
			raise ValueError("radius is subzero")
		return tuple.__new__(subtype, (Point(center), float(radius)))

	def __repr__(self):
		return '%s.%s((%s, %s))' %(
			type(self).__module__,
			type(self).__name__,
			tuple.__repr__(self[0]),
			repr(self[1])
		)

	def __str__(self):
		return '<%s,%s>' %(self[0], self[1])
