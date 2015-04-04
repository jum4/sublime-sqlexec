from .. import BITOID, VARBITOID
from ..bitwise import Varbit, Bit
from . import lib

def varbit_pack(x, pack = lib.varbit_pack):
	return pack((x.bits, x.data))

def varbit_unpack(x, unpack = lib.varbit_unpack):
	return Varbit.from_bits(*unpack(x))

oid_to_io = {
	BITOID : (varbit_pack, varbit_unpack, Bit),
	VARBITOID : (varbit_pack, varbit_unpack, Varbit),
}

oid_to_type = {
	BITOID : Bit,
	VARBITOID : Varbit,
}
