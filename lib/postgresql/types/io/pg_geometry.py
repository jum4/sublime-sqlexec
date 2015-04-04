from .. import POINTOID, BOXOID, LSEGOID, CIRCLEOID
from ..geometry import Point, Box, Lseg, Circle
from ...python.functools import Composition as compose
from . import lib

oid_to_type = {
	POINTOID: Point,
	BOXOID: Box,
	LSEGOID: Lseg,
	CIRCLEOID: Circle,
}

# Make a pair of pairs out of a sequence of four objects
def two_pair(x):
	return ((x[0], x[1]), (x[2], x[3]))

point_pack = lib.point_pack
point_unpack = compose((lib.point_unpack, Point))

def box_pack(x):
	return lib.box_pack((x[0][0], x[0][1], x[1][0], x[1][1]))
box_unpack = compose((lib.box_unpack, two_pair, Box,))

def lseg_pack(x, pack = lib.lseg_pack):
	return pack((x[0][0], x[0][1], x[1][0], x[1][1]))
lseg_unpack = compose((lib.lseg_unpack, two_pair, Lseg))

def circle_pack(x):
	return lib.circle_pack((x[0][0], x[0][1], x[1]))
def circle_unpack(x, unpack = lib.circle_unpack, Circle = Circle):
	x = unpack(x)
	return Circle(((x[0], x[1]), x[2]))

# Map type oids to a (pack, unpack) pair.
oid_to_io = {
	POINTOID : (point_pack, point_unpack, Point),
	BOXOID : (box_pack, box_unpack, Box),
	LSEGOID : (lseg_pack, lseg_unpack, Lseg),
	CIRCLEOID : (circle_pack, circle_unpack, Circle),
	#PATHOID : (path_pack, path_unpack),
	#POLYGONOID : (polygon_pack, polygon_unpack),
	#LINEOID : (line_pack, line_unpack),
}
