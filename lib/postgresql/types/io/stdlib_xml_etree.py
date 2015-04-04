##
# types.io.stdlib_xml_etree
##
try:
	import xml.etree.cElementTree as etree
except ImportError:
	import xml.etree.ElementTree as etree
from .. import XMLOID
from ...python.functools import Composition as compose

oid_to_type = {
	XMLOID: etree.ElementTree,
}

def xml_unpack(xmldata, XML = etree.XML):
	try:
		return XML(xmldata)
	except Exception:
		# try it again, but return the sequence of children.
		return tuple(XML('<x>' + xmldata + '</x>'))

if not hasattr(etree, 'tostringlist'):
	# Python 3.1 support.
	def xml_pack(xml, tostr = etree.tostring, et = etree.ElementTree,
		str = str, isinstance = isinstance, tuple = tuple
	):
		if isinstance(xml, str):
			# If it's a string, encode and return.
			return xml
		elif isinstance(xml, tuple):
			# If it's a tuple, encode and return the joined items.
			# We do not accept lists here--emphasizing lists being used for ARRAY
			# bounds.
			return ''.join((x if isinstance(x, str) else tostr(x) for x in xml))
		return tostr(xml)

	def xml_io_factory(typoid, typio, c = compose):
		return (
			c((xml_pack, typio.encode)),
			c((typio.decode, xml_unpack)),
			etree.ElementTree,
		)
else:
	# New etree tostring API.
	def xml_pack(xml, encoding, encoder,
		tostr = etree.tostring, et = etree.ElementTree,
		str = str, isinstance = isinstance, tuple = tuple,
	):
		if isinstance(xml, bytes):
			return xml
		if isinstance(xml, str):
			# If it's a string, encode and return.
			return encoder(xml)
		elif isinstance(xml, tuple):
			# If it's a tuple, encode and return the joined items.
			# We do not accept lists here--emphasizing lists being used for ARRAY
			# bounds.
			##
			# 3.2
			# XXX: tostring doesn't include declaration with utf-8?
			x = b''.join(
				x.encode('utf-8') if isinstance(x, str) else
				tostr(x, encoding = "utf-8")
				for x in xml
			)
		else:
			##
			# 3.2
			# XXX: tostring doesn't include declaration with utf-8?
			x = tostr(xml, encoding = "utf-8")
		if encoding in ('utf8','utf-8'):
			return x
		else:
			return encoder(x.decode('utf-8'))

	def xml_io_factory(typoid, typio, c = compose):
		def local_xml_pack(x, encoder = typio.encode, typio = typio, xml_pack = xml_pack):
			return xml_pack(x, typio.encoding, encoder)
		return (local_xml_pack, c((typio.decode, xml_unpack)), etree.ElementTree,)

oid_to_io = {
	XMLOID : xml_io_factory
}
