import uuid
from ...types import UUIDOID

def uuid_pack(x, UUID = uuid.UUID, bytes = bytes):
	if isinstance(x, UUID):
		return bytes(x.bytes)
	return bytes(UUID(x).bytes)

def uuid_unpack(x, UUID = uuid.UUID):
	return UUID(bytes=x)

oid_to_io = {
	UUIDOID : (uuid_pack, uuid_unpack),
}
