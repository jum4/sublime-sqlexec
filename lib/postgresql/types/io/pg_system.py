from ...types import OIDOID, XIDOID, CIDOID, TIDOID
from . import lib

oid_to_io = {
	OIDOID : (lib.oid_pack, lib.oid_unpack),
	XIDOID : (lib.xid_pack, lib.xid_unpack),
	CIDOID : (lib.cid_pack, lib.cid_unpack),
	TIDOID : (lib.tid_pack, lib.tid_unpack),
	#ACLITEMOID : (aclitem_pack, aclitem_unpack),
}
