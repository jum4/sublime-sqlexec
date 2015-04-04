from .. import \
	INT2OID, INT4OID, INT8OID, \
	BOOLOID, BYTEAOID, CHAROID, \
	ABSTIMEOID, FLOAT4OID, FLOAT8OID, \
	TEXTOID, BPCHAROID, NAMEOID, VARCHAROID
from . import lib

bool_pack = {True:b'\x01', False:b'\x00'}.__getitem__
bool_unpack = {b'\x01':True, b'\x00':False}.__getitem__

int2_pack, int2_unpack = lib.short_pack, lib.short_unpack
int4_pack, int4_unpack = lib.long_pack, lib.long_unpack
int8_pack, int8_unpack = lib.longlong_pack, lib.longlong_unpack

bytea_pack = bytes
bytea_unpack = bytes
char_pack = bytes
char_unpack = bytes

oid_to_io = {
	BOOLOID : (bool_pack, bool_unpack, bool),

	BYTEAOID : (bytea_pack, bytea_unpack, bytes),
	CHAROID : (char_pack, char_unpack, bytes),

	INT2OID : (int2_pack, int2_unpack, int),
	INT4OID : (int4_pack, int4_unpack, int),
	INT8OID : (int8_pack, int8_unpack, int),

	ABSTIMEOID : (lib.long_pack, lib.long_unpack, int),
	FLOAT4OID : (lib.float_pack, lib.float_unpack, float),
	FLOAT8OID : (lib.double_pack, lib.double_unpack, float),
}

# Python Representations of PostgreSQL Types
oid_to_type = {
	BOOLOID: bool,

	VARCHAROID: str,
	TEXTOID: str,
	BPCHAROID: str,
	NAMEOID: str,

	# This is *not* bpchar, the SQL CHARACTER type.
	CHAROID: bytes,
	BYTEAOID: bytes,

	INT2OID: int,
	INT4OID: int,
	INT8OID: int,

	FLOAT4OID: float,
	FLOAT8OID: float,
}
