##
# .test.test_driver
##
import sys
import unittest
import gc
import threading
import time
import datetime
import decimal
import uuid
from itertools import chain, islice
from operator import itemgetter

from ..python.datetime import FixedOffset, \
	negative_infinity_datetime, infinity_datetime, \
	negative_infinity_date, infinity_date
from .. import types as pg_types
from ..types.io.stdlib_xml_etree import etree
from .. import exceptions as pg_exc
from ..types.bitwise import Bit, Varbit
from ..temporal import pg_tmp

type_samples = [
	('smallint', (
			((1 << 16) // 2) - 1, - ((1 << 16) // 2),
			-1, 0, 1,
		),
	),
	('int', (
			((1 << 32) // 2) - 1, - ((1 << 32) // 2),
			-1, 0, 1,
		),
	),
	('bigint', (
			((1 << 64) // 2) - 1, - ((1 << 64) // 2),
			-1, 0, 1,
		),
	),
	('numeric', (
			-(2**64),
			2**64,
			-(2**128),
			2**128,
			-1, 0, 1,
			decimal.Decimal("0.00000000000000"),
			decimal.Decimal("1.00000000000000"),
			decimal.Decimal("-1.00000000000000"),
			decimal.Decimal("-2.00000000000000"),
			decimal.Decimal("1000000000000000.00000000000000"),
			decimal.Decimal("-0.00000000000000"),
			decimal.Decimal(1234),
			decimal.Decimal(-1234),
			decimal.Decimal("1234000000.00088883231"),
			decimal.Decimal(str(1234.00088883231)),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("-3123000000.23111"),
			decimal.Decimal("3123.2311100000"),
			decimal.Decimal("-03123.0023111"),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("3123.23111"),
			decimal.Decimal("10000.23111"),
			decimal.Decimal("100000.23111"),
			decimal.Decimal("1000000.23111"),
			decimal.Decimal("10000000.23111"),
			decimal.Decimal("100000000.23111"),
			decimal.Decimal("1000000000.23111"),
			decimal.Decimal("1000000000.3111"),
			decimal.Decimal("1000000000.111"),
			decimal.Decimal("1000000000.11"),
			decimal.Decimal("100000000.0"),
			decimal.Decimal("10000000.0"),
			decimal.Decimal("1000000.0"),
			decimal.Decimal("100000.0"),
			decimal.Decimal("10000.0"),
			decimal.Decimal("1000.0"),
			decimal.Decimal("100.0"),
			decimal.Decimal("100"),
			decimal.Decimal("100.1"),
			decimal.Decimal("100.12"),
			decimal.Decimal("100.123"),
			decimal.Decimal("100.1234"),
			decimal.Decimal("100.12345"),
			decimal.Decimal("100.123456"),
			decimal.Decimal("100.1234567"),
			decimal.Decimal("100.12345679"),
			decimal.Decimal("100.123456790"),
			decimal.Decimal("100.123456790000000000000000"),
			decimal.Decimal("1.0"),
			decimal.Decimal("0.0"),
			decimal.Decimal("-1.0"),
			decimal.Decimal("1.0E-1000"),
			decimal.Decimal("1.0E1000"),
			decimal.Decimal("1.0E10000"),
			decimal.Decimal("1.0E-10000"),
			decimal.Decimal("1.0E15000"),
			decimal.Decimal("1.0E-15000"),
			decimal.Decimal("1.0E-16382"),
			decimal.Decimal("1.0E32767"),
			decimal.Decimal("0.000000000000000000000000001"),
			decimal.Decimal("0.000000000000010000000000001"),
			decimal.Decimal("0.00000000000000000000000001"),
			decimal.Decimal("0.00000000100000000000000001"),
			decimal.Decimal("0.0000000000000000000000001"),
			decimal.Decimal("0.000000000000000000000001"),
			decimal.Decimal("0.00000000000000000000001"),
			decimal.Decimal("0.0000000000000000000001"),
			decimal.Decimal("0.000000000000000000001"),
			decimal.Decimal("0.00000000000000000001"),
			decimal.Decimal("0.0000000000000000001"),
			decimal.Decimal("0.000000000000000001"),
			decimal.Decimal("0.00000000000000001"),
			decimal.Decimal("0.0000000000000001"),
			decimal.Decimal("0.000000000000001"),
			decimal.Decimal("0.00000000000001"),
			decimal.Decimal("0.0000000000001"),
			decimal.Decimal("0.000000000001"),
			decimal.Decimal("0.00000000001"),
			decimal.Decimal("0.0000000001"),
			decimal.Decimal("0.000000001"),
			decimal.Decimal("0.00000001"),
			decimal.Decimal("0.0000001"),
			decimal.Decimal("0.000001"),
			decimal.Decimal("0.00001"),
			decimal.Decimal("0.0001"),
			decimal.Decimal("0.001"),
			decimal.Decimal("0.01"),
			decimal.Decimal("0.1"),
			# these require some weight transfer
		),
	),
	('bytea', (
			bytes(range(256)),
			bytes(range(255, -1, -1)),
			b'\x00\x00',
			b'foo',
		),
	),
	('smallint[]', (
			[123,321,-123,-321],
			[],
		),
	),
	('int[]', [
			[123,321,-123,-321],
			[[1],[2]],
			[],
		],
	),
	('bigint[]', [
			[
				0,
				1,
				-1,
				0xFFFFFFFFFFFF,
				-0xFFFFFFFFFFFF,
				((1 << 64) // 2) - 1,
				- ((1 << 64) // 2),
			],
			[],
		],
	),
	('varchar[]', [
			["foo", "bar",],
			["foo", "bar",],
			[],
		],
	),
	('timestamp', [
			datetime.datetime(3000,5,20,5,30,10),
			datetime.datetime(2000,1,1,5,25,10),
			datetime.datetime(500,1,1,5,25,10),
			datetime.datetime(250,1,1,5,25,10),
			infinity_datetime,
			negative_infinity_datetime,
		],
	),
	('date', [
			datetime.date(3000,5,20),
			datetime.date(2000,1,1),
			datetime.date(500,1,1),
			datetime.date(1,1,1),
		],
	),
	('time', [
			datetime.time(12,15,20),
			datetime.time(0,1,1),
			datetime.time(23,59,59),
		],
	),
	('timestamptz', [
			# It's converted to UTC. When it comes back out, it will be in UTC
			# again. The datetime comparison will take the tzinfo into account.
			datetime.datetime(1990,5,12,10,10,0, tzinfo=FixedOffset(4000)),
			datetime.datetime(1982,5,18,10,10,0, tzinfo=FixedOffset(6000)),
			datetime.datetime(1950,1,1,10,10,0, tzinfo=FixedOffset(7000)),
			datetime.datetime(1800,1,1,10,10,0, tzinfo=FixedOffset(2000)),
			datetime.datetime(2400,1,1,10,10,0, tzinfo=FixedOffset(2000)),
			infinity_datetime,
			negative_infinity_datetime,
		],
	),
	('timetz', [
			# timetz retains the offset
			datetime.time(10,10,0, tzinfo=FixedOffset(4000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(6000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(7000)),
			datetime.time(10,10,0, tzinfo=FixedOffset(2000)),
			datetime.time(22,30,0, tzinfo=FixedOffset(0)),
		],
	),
	('interval', [
			# no months :(
			datetime.timedelta(40, 10, 1234),
			datetime.timedelta(0, 0, 4321),
			datetime.timedelta(0, 0),
			datetime.timedelta(-100, 0),
			datetime.timedelta(-100, -400),
		],
	),
	('point', [
			(10, 1234),
			(-1, -1),
			(0, 0),
			(1, 1),
			(-100, 0),
			(-100, -400),
			(-100.02314, -400.930425),
			(0xFFFF, 1.3124243),
		],
	),
	('lseg', [
			((0,0),(0,0)),
			((10,5),(18,293)),
			((55,5),(10,293)),
			((-1,-1),(-1,-1)),
			((-100,0.00231),(50,45.42132)),
			((0.123,0.00231),(50,45.42132)),
		],
	),
	('circle', [
			((0,0),0),
			((0,0),1),
			((0,0),1.0011),
			((1,1),1.0011),
			((-1,-1),1.0011),
			((1,-1),1.0011),
			((-1,1),1.0011),
		],
	),
	('box', [
			((0,0),(0,0)),
			((-1,-1),(-1,-1)),
			((1,1),(-1,-1)),
			((10,1),(-1,-1)),
			((100.2312,45.1232),(-123.023,-1423.82342)),
		],
	),
	('bit', [
			Bit('1'),
			Bit('0'),
			None,
		],
	),
	('varbit', [
			Varbit('1'),
			Varbit('0'),
			Varbit('10'),
			Varbit('11'),
			Varbit('00'),
			Varbit('001'),
			Varbit('101'),
			Varbit('111'),
			Varbit('0010'),
			Varbit('1010'),
			Varbit('1010'),
			Varbit('01010101011111011010110101010101111'),
			Varbit('010111101111'),
		],
	),
	('macaddr[]', [
			['00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff'],
			['00:00:00:00:00:01', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff'],
			['00:00:00:00:00:01', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff', '10:00:00:00:00:00'],
		],
	),
]

try:
	import ipaddress

	type_samples.extend([
		('inet', [
				ipaddress.IPv4Address('255.255.255.255'),
				ipaddress.IPv4Address('127.0.0.1'),
				ipaddress.IPv4Address('10.0.0.1'),
				ipaddress.IPv4Address('0.0.0.0'),
				ipaddress.IPv6Address('::1'),
				ipaddress.IPv6Address('ffff' + ':ffff'*7),
				ipaddress.IPv6Address('fe80::1'),
				ipaddress.IPv6Address('fe80::1'),
				ipaddress.IPv6Address('::'), # 0::0
			],
		),
		('cidr', [
				ipaddress.IPv4Network('255.255.255.255/32'),
				ipaddress.IPv4Network('127.0.0.0/8'),
				ipaddress.IPv4Network('127.1.0.0/16'),
				ipaddress.IPv4Network('10.0.0.0/32'),
				ipaddress.IPv4Network('0.0.0.0/0'),
				ipaddress.IPv6Network('ffff' + ':ffff'*7 + '/128'),
				ipaddress.IPv6Network('::1/128'),
				ipaddress.IPv6Network('fe80::1/128'),
				ipaddress.IPv6Network('fe80::/64'),
				ipaddress.IPv6Network('fe80::/16'),
				ipaddress.IPv6Network('::/0'),
			],
		),
		('inet[]', [
				[ipaddress.IPv4Address('127.0.0.1'), ipaddress.IPv6Address('::1')],
				[ipaddress.IPv4Address('10.0.0.1'), ipaddress.IPv6Address('fe80::1')],
			],
		),
		('cidr[]', [
				[ipaddress.IPv4Network('127.0.0.0/8'), ipaddress.IPv6Network('::/0')],
				[ipaddress.IPv4Network('10.0.0.0/16'), ipaddress.IPv6Network('fe80::/64')],
				[ipaddress.IPv4Network('10.102.0.0/16'), ipaddress.IPv6Network('fe80::/64')],
			],
		),
	])
except ImportError:
	pass

class test_driver(unittest.TestCase):
	@pg_tmp
	def testInterrupt(self):
		def pg_sleep(l):
			try:
				db.execute("SELECT pg_sleep(5)")
			except Exception:
				l.append(sys.exc_info())
			else:
				l.append(None)
				return
		rl = []
		t = threading.Thread(target = pg_sleep, args = (rl,))
		t.start()
		time.sleep(0.2)
		while t.is_alive():
			db.interrupt()
			time.sleep(0.1)

		def raise_exc(l):
			if l[0] is not None:
				e, v, tb = rl[0]
				raise v
		self.assertRaises(pg_exc.QueryCanceledError, raise_exc, rl)

	@pg_tmp
	def testClones(self):
		db.execute('create table _can_clone_see_this (i int);')
		try:
			with db.clone() as db2:
				self.assertEqual(db2.prepare('select 1').first(), 1)
				self.assertEqual(db2.prepare(
						"select count(*) FROM information_schema.tables " \
						"where table_name = '_can_clone_see_this'"
					).first(), 1
				)
		finally:
			db.execute('drop table _can_clone_see_this')

		# check already open
		db3 = db.clone()
		try:
			self.assertEqual(db3.prepare('select 1').first(), 1)
		finally:
			db3.close()

		ps = db.prepare('select 1')
		ps2 = ps.clone()
		self.assertEqual(ps2.first(), ps.first())
		ps2.close()
		c = ps.declare()
		c2 = c.clone()
		self.assertEqual(c.read(), c2.read())

	@pg_tmp
	def testItsClosed(self):
		ps = db.prepare("SELECT 1")
		# If scroll is False it will pre-fetch, and no error will be thrown.
		c = ps.declare()
		#
		c.close()
		self.assertRaises(pg_exc.CursorNameError, c.read)
		self.assertEqual(ps.first(), 1)
		#
		ps.close()
		self.assertRaises(pg_exc.StatementNameError, ps.first)
		#
		db.close()
		self.assertRaises(
			pg_exc.ConnectionDoesNotExistError,
			db.execute, "foo"
		)
		# No errors, it's already closed.
		ps.close()
		c.close()
		db.close()

	@pg_tmp
	def testGarbage(self):
		ps = db.prepare('select 1')
		sid = ps.statement_id
		ci = ps.chunks()
		ci_id = ci.cursor_id
		c = ps.declare()
		cid = c.cursor_id
		# make sure there are no remaining xact references..
		db._pq_complete()
		# ci and c both hold references to ps, so they must
		# be removed before we can observe the effects __del__
		del c
		gc.collect()
		self.assertTrue(db.typio.encode(cid) in db.pq.garbage_cursors)
		del ci
		gc.collect()
		self.assertTrue(db.typio.encode(ci_id) in db.pq.garbage_cursors)
		del ps
		gc.collect()
		self.assertTrue(db.typio.encode(sid) in db.pq.garbage_statements)

	@pg_tmp
	def testStatementCall(self):
		ps = db.prepare("SELECT 1")
		r = ps()
		self.assertTrue(isinstance(r, list))
		self.assertEqual(ps(), [(1,)])
		ps = db.prepare("SELECT 1, 2")
		self.assertEqual(ps(), [(1,2)])
		ps = db.prepare("SELECT 1, 2 UNION ALL SELECT 3, 4")
		self.assertEqual(ps(), [(1,2),(3,4)])

	@pg_tmp
	def testStatementFirstDML(self):
		cmd = prepare("CREATE TEMP TABLE first (i int)").first()
		self.assertEqual(cmd, 'CREATE TABLE')
		fins = db.prepare("INSERT INTO first VALUES (123)").first
		fupd = db.prepare("UPDATE first SET i = 321 WHERE i = 123").first
		fdel = db.prepare("DELETE FROM first").first
		self.assertEqual(fins(), 1)
		self.assertEqual(fdel(), 1)
		self.assertEqual(fins(), 1)
		self.assertEqual(fupd(), 1)
		self.assertEqual(fins(), 1)
		self.assertEqual(fins(), 1)
		self.assertEqual(fupd(), 2)
		self.assertEqual(fdel(), 3)
		self.assertEqual(fdel(), 0)

	@pg_tmp
	def testStatementRowsPersistence(self):
		# validate that rows' cursor will persist beyond a transaction.
		ps = db.prepare("SELECT i FROM generate_series($1::int, $2::int) AS g(i)")
		# create the iterator inside the transaction
		rows = ps.rows(0, 10000-1)
		ps(0,1)
		# validate the first half.
		self.assertEqual(
			list(islice(map(itemgetter(0), rows), 5000)),
			list(range(5000))
		)
		ps(0,1)
		# and the second half.
		self.assertEqual(
			list(map(itemgetter(0), rows)),
			list(range(5000, 10000))
		)

	@pg_tmp
	def testStatementParameters(self):
		# too few and takes one
		ps = db.prepare("select $1::integer")
		self.assertRaises(TypeError, ps)

		# too many and takes one
		self.assertRaises(TypeError, ps, 1, 2)

		# too many and takes none
		ps = db.prepare("select 1")
		self.assertRaises(TypeError, ps, 1)

		# too many and takes some
		ps = db.prepare("select $1::int, $2::text")
		self.assertRaises(TypeError, ps, 1, "foo", "bar")

	@pg_tmp
	def testStatementAndCursorMetadata(self):
		ps = db.prepare("SELECT $1::integer AS my_int_column")
		self.assertEqual(tuple(ps.column_names), ('my_int_column',))
		self.assertEqual(tuple(ps.sql_column_types), ('INTEGER',))
		self.assertEqual(tuple(ps.sql_parameter_types), ('INTEGER',))
		self.assertEqual(tuple(ps.pg_parameter_types), (pg_types.INT4OID,))
		self.assertEqual(tuple(ps.parameter_types), (int,))
		self.assertEqual(tuple(ps.column_types), (int,))
		c = ps.declare(15)
		self.assertEqual(tuple(c.column_names), ('my_int_column',))
		self.assertEqual(tuple(c.sql_column_types), ('INTEGER',))
		self.assertEqual(tuple(c.column_types), (int,))

		ps = db.prepare("SELECT $1::text AS my_text_column")
		self.assertEqual(tuple(ps.column_names), ('my_text_column',))
		self.assertEqual(tuple(ps.sql_column_types), ('pg_catalog.text',))
		self.assertEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text',))
		self.assertEqual(tuple(ps.pg_parameter_types), (pg_types.TEXTOID,))
		self.assertEqual(tuple(ps.column_types), (str,))
		self.assertEqual(tuple(ps.parameter_types), (str,))
		c = ps.declare('textdata')
		self.assertEqual(tuple(c.column_names), ('my_text_column',))
		self.assertEqual(tuple(c.sql_column_types), ('pg_catalog.text',))
		self.assertEqual(tuple(c.pg_column_types), (pg_types.TEXTOID,))
		self.assertEqual(tuple(c.column_types), (str,))

		ps = db.prepare("SELECT $1::text AS my_column1, $2::varchar AS my_column2")
		self.assertEqual(tuple(ps.column_names), ('my_column1','my_column2'))
		self.assertEqual(tuple(ps.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.assertEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.assertEqual(tuple(ps.pg_parameter_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.assertEqual(tuple(ps.pg_column_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.assertEqual(tuple(ps.parameter_types), (str,str))
		self.assertEqual(tuple(ps.column_types), (str,str))
		c = ps.declare('textdata', 'varchardata')
		self.assertEqual(tuple(c.column_names), ('my_column1','my_column2'))
		self.assertEqual(tuple(c.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING'))
		self.assertEqual(tuple(c.pg_column_types), (pg_types.TEXTOID, pg_types.VARCHAROID))
		self.assertEqual(tuple(c.column_types), (str,str))

		db.execute("CREATE TYPE public.myudt AS (i int)")
		myudt_oid = db.prepare("select oid from pg_type WHERE typname='myudt'").first()
		ps = db.prepare("SELECT $1::text AS my_column1, $2::varchar AS my_column2, $3::public.myudt AS my_column3")
		self.assertEqual(tuple(ps.column_names), ('my_column1','my_column2', 'my_column3'))
		self.assertEqual(tuple(ps.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING', '"public"."myudt"'))
		self.assertEqual(tuple(ps.sql_parameter_types), ('pg_catalog.text', 'CHARACTER VARYING', '"public"."myudt"'))
		self.assertEqual(tuple(ps.pg_column_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid)
		)
		self.assertEqual(tuple(ps.pg_parameter_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid)
		)
		self.assertEqual(tuple(ps.parameter_types), (str,str,tuple))
		self.assertEqual(tuple(ps.column_types), (str,str,tuple))
		c = ps.declare('textdata', 'varchardata', (123,))
		self.assertEqual(tuple(c.column_names), ('my_column1','my_column2', 'my_column3'))
		self.assertEqual(tuple(c.sql_column_types), ('pg_catalog.text', 'CHARACTER VARYING', '"public"."myudt"'))
		self.assertEqual(tuple(c.pg_column_types), (
			pg_types.TEXTOID, pg_types.VARCHAROID, myudt_oid
		))
		self.assertEqual(tuple(c.column_types), (str,str,tuple))

	@pg_tmp
	def testRowInterface(self):
		data = (1, '0', decimal.Decimal('0.00'), datetime.datetime(1982,5,18,12,30,0))
		ps = db.prepare(
			"SELECT 1::int2 AS col0, " \
			"'0'::text AS col1, 0.00::numeric as col2, " \
			"'1982-05-18 12:30:00'::timestamp as col3;"
		)
		row = ps.first()
		self.assertEqual(tuple(row), data)

		self.assertTrue(1 in row)
		self.assertTrue('0' in row)
		self.assertTrue(decimal.Decimal('0.00') in row)
		self.assertTrue(datetime.datetime(1982,5,18,12,30,0) in row)

		self.assertEqual(
			tuple(row.column_names),
			tuple(['col' + str(i) for i in range(4)])
		)
		self.assertEqual(
			(row["col0"], row["col1"], row["col2"], row["col3"]),
			(row[0], row[1], row[2], row[3]),
		)
		self.assertEqual(
			(row["col0"], row["col1"], row["col2"], row["col3"]),
			(row[0], row[1], row[2], row[3]),
		)
		keys = list(row.keys())
		cnames = list(ps.column_names)
		cnames.sort()
		keys.sort()
		self.assertEqual(keys, cnames)
		self.assertEqual(list(row.values()), list(data))
		self.assertEqual(list(row.items()), list(zip(ps.column_names, data)))

		row_d = dict(row)
		for x in ps.column_names:
			self.assertEqual(row_d[x], row[x])
		for x in row_d.keys():
			self.assertEqual(row.get(x), row_d[x])

		row_t = tuple(row)
		self.assertEqual(row_t, row)

		# transform
		crow = row.transform(col0 = str)
		self.assertEqual(type(crow[0]), str)
		crow = row.transform(str)
		self.assertEqual(type(crow[0]), str)
		crow = row.transform(str, int)
		self.assertEqual(type(crow[0]), str)
		self.assertEqual(type(crow[1]), int)
		# None = no transformation
		crow = row.transform(None, int)
		self.assertEqual(type(crow[0]), int)
		self.assertEqual(type(crow[1]), int)
		# and a combination
		crow = row.transform(str, col1 = int, col3 = str)
		self.assertEqual(type(crow[0]), str)
		self.assertEqual(type(crow[1]), int)
		self.assertEqual(type(crow[3]), str)

		for i in range(4):
			self.assertEqual(i, row.index_from_key('col' + str(i)))
			self.assertEqual('col' + str(i), row.key_from_index(i))

	def column_test(self):
		g_i = db.prepare('SELECT i FROM generate_series(1,10) as g(i)').column
		# ignore the second column.
		g_ii = db.prepare('SELECT i, i+10 as i2 FROM generate_series(1,10) as g(i)').column
		self.assertEqual(tuple(g_i()), tuple(g_ii()))
		self.assertEqual(tuple(g_i()), (1,2,3,4,5,6,7,8,9,10))

	@pg_tmp
	def testColumn(self):
		self.column_test()

	@pg_tmp
	def testColumnInXact(self):
		with db.xact():
			self.column_test()

	@pg_tmp
	def testStatementFromId(self):
		db.execute("PREPARE foo AS SELECT 1 AS colname;")
		ps = db.statement_from_id('foo')
		self.assertEqual(ps.first(), 1)
		self.assertEqual(ps(), [(1,)])
		self.assertEqual(list(ps), [(1,)])
		self.assertEqual(tuple(ps.column_names), ('colname',))

	@pg_tmp
	def testCursorFromId(self):
		db.execute("DECLARE foo CURSOR WITH HOLD FOR SELECT 1")
		c = db.cursor_from_id('foo')
		self.assertEqual(c.read(), [(1,)])
		db.execute(
			"DECLARE bar SCROLL CURSOR WITH HOLD FOR SELECT i FROM generate_series(0, 99) AS g(i)"
		)
		c = db.cursor_from_id('bar')
		c.seek(50)
		self.assertEqual([x for x, in c.read(10)], list(range(50,60)))
		c.seek(0,2)
		self.assertEqual(c.read(), [])
		c.seek(0)
		self.assertEqual([x for x, in c.read()], list(range(100)))

	@pg_tmp
	def testCopyToSTDOUT(self):
		with db.xact():
			db.execute("CREATE TABLE foo (i int)")
			foo = db.prepare('insert into foo values ($1)')
			foo.load_rows(((x,) for x in range(500)))

			copy_foo = db.prepare('copy foo to stdout')
			foo_content = set(copy_foo)
			expected = set((str(i).encode('ascii') + b'\n' for i in range(500)))
			self.assertEqual(expected, foo_content)
			self.assertEqual(expected, set(copy_foo()))
			self.assertEqual(expected, set(chain.from_iterable(copy_foo.chunks())))
			self.assertEqual(expected, set(copy_foo.rows()))
			db.execute("DROP TABLE foo")

	@pg_tmp
	def testCopyFromSTDIN(self):
		with db.xact():
			db.execute("CREATE TABLE foo (i int)")
			foo = db.prepare('copy foo from stdin')
			foo.load_rows((str(i).encode('ascii') + b'\n' for i in range(200)))
			foo_content = list((
				x for (x,) in db.prepare('select * from foo order by 1 ASC')
			))
			self.assertEqual(foo_content, list(range(200)))
			db.execute("DROP TABLE foo")

	@pg_tmp
	def testCopyInvalidTermination(self):
		class DontTrapThis(BaseException):
			pass
		def EvilGenerator():
			raise DontTrapThis()
			yield None
		sqlexec("CREATE TABLE foo (i int)")
		foo = prepare('copy foo from stdin')
		try:
			foo.load_chunks([EvilGenerator()])
			self.fail("didn't raise the BaseException subclass")
		except DontTrapThis:
			pass
		try:
			db._pq_complete()
		except Exception:
			pass
		self.assertEqual(prepare('select 1').first(), 1)

	@pg_tmp
	def testLookupProcByName(self):
		db.execute(
			"CREATE OR REPLACE FUNCTION public.foo() RETURNS INT LANGUAGE SQL AS 'SELECT 1'"
		)
		db.settings['search_path'] = 'public'
		f = db.proc('foo()')
		f2 = db.proc('public.foo()')
		self.assertTrue(f.oid == f2.oid,
			"function lookup incongruence(%r != %r)" %(f, f2)
		)

	@pg_tmp
	def testLookupProcById(self):
		gsoid = db.prepare(
			"select oid from pg_proc where proname = 'generate_series' limit 1"
		).first()
		gs = db.proc(gsoid)
		self.assertEqual(list(gs(1, 100)), list(range(1, 101)))

	def execute_proc(self):
		ver = db.proc("version()")
		ver()
		db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(int) RETURNS int LANGUAGE SQL AS 'select $1'"
		)
		ifoo = db.proc('ifoo(int)')
		self.assertEqual(ifoo(1), 1)
		self.assertEqual(ifoo(None), None)
		db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(varchar) RETURNS text LANGUAGE SQL AS 'select $1'"
		)
		ifoo = db.proc('ifoo(varchar)')
		self.assertEqual(ifoo('1'), '1')
		self.assertEqual(ifoo(None), None)
		db.execute(
			"CREATE OR REPLACE FUNCTION ifoo(varchar,int) RETURNS text LANGUAGE SQL AS 'select ($1::int + $2)::varchar'"
		)
		ifoo = db.proc('ifoo(varchar,int)')
		self.assertEqual(ifoo('1',1), '2')
		self.assertEqual(ifoo(None,1), None)
		self.assertEqual(ifoo('1',None), None)
		self.assertEqual(ifoo('2',2), '4')

	@pg_tmp
	def testProcExecution(self):
		self.execute_proc()

	@pg_tmp
	def testProcExecutionInXact(self):
		with db.xact():
			self.execute_proc()

	@pg_tmp
	def testProcExecutionInSubXact(self):
		with db.xact(), db.xact():
			self.execute_proc()

	@pg_tmp
	def testNULL(self):
		# Directly commpare (SELECT NULL) is None
		self.assertTrue(
			db.prepare("SELECT NULL")()[0][0] is None,
			"SELECT NULL did not return None"
		)
		# Indirectly compare (select NULL) is None
		self.assertTrue(
			db.prepare("SELECT $1::text")(None)[0][0] is None,
			"[SELECT $1::text](None) did not return None"
		)

	@pg_tmp
	def testBool(self):
		fst, snd = db.prepare("SELECT true, false").first()
		self.assertTrue(fst is True)
		self.assertTrue(snd is False)

	def select(self):
		#self.assertEqual(
		#	db.prepare('')().command(),
		#	None,
		#	'Empty statement has command?'
		#)
		# Test SELECT 1.
		s1 = db.prepare("SELECT 1 as name")
		p = s1()
		tup = p[0]
		self.assertTrue(tup[0] == 1)

		for tup in s1:
			self.assertEqual(tup[0], 1)

		for tup in s1:
			self.assertEqual(tup["name"], 1)

	@pg_tmp
	def testSelect(self):
		self.select()

	@pg_tmp
	def testSelectInXact(self):
		with db.xact():
			self.select()

	def cursor_read(self):
		ps = db.prepare("SELECT i FROM generate_series(0, (2^8)::int - 1) AS g(i)")
		c = ps.declare()
		self.assertEqual(c.read(0), [])
		self.assertEqual(c.read(0), [])
		self.assertEqual(c.read(1), [(0,)])
		self.assertEqual(c.read(1), [(1,)])
		self.assertEqual(c.read(2), [(2,), (3,)])
		self.assertEqual(c.read(2), [(4,), (5,)])
		self.assertEqual(c.read(3), [(6,), (7,), (8,)])
		self.assertEqual(c.read(4), [(9,), (10,), (11,), (12,)])
		self.assertEqual(c.read(4), [(13,), (14,), (15,), (16,)])
		self.assertEqual(c.read(5), [(17,), (18,), (19,), (20,), (21,)])
		self.assertEqual(c.read(0), [])
		self.assertEqual(c.read(6), [(22,),(23,),(24,),(25,),(26,),(27,)])
		r = [-1]
		i = 4
		v = 28
		maxv = 2**8
		while r:
			i = i * 2
			r = [x for x, in c.read(i)]
			top = min(maxv, v + i)
			self.assertEqual(r, list(range(v, top)))
			v = top

	@pg_tmp
	def testCursorRead(self):
		self.cursor_read()

	@pg_tmp
	def testCursorIter(self):
		ps = db.prepare("SELECT i FROM generate_series(0, 10) AS g(i)")
		c = ps.declare()
		self.assertEqual(next(iter(c)), (0,))
		self.assertEqual(next(iter(c)), (1,))
		self.assertEqual(next(iter(c)), (2,))

	@pg_tmp
	def testCursorReadInXact(self):
		with db.xact():
			self.cursor_read()

	@pg_tmp
	def testScroll(self, direction = True):
		# Use a large row-set.
		imin = 0
		imax = 2**16
		if direction:
			ps = db.prepare("SELECT i FROM generate_series(0, (2^16)::int) AS g(i)")
		else:
			ps = db.prepare("SELECT i FROM generate_series((2^16)::int, 0, -1) AS g(i)")
		c = ps.declare()
		c.direction = direction
		if not direction:
			c.seek(0)

		self.assertEqual([x for x, in c.read(10)], list(range(10)))
		# bit strange to me, but i've watched the fetch backwards -jwp 2009
		self.assertEqual([x for x, in c.read(10, 'BACKWARD')], list(range(8, -1, -1)))
		c.seek(0, 2)
		self.assertEqual([x for x, in c.read(10, 'BACKWARD')], list(range(imax, imax-10, -1)))

		# move to end
		c.seek(0, 2)
		self.assertEqual([x for x, in c.read(100, 'BACKWARD')], list(range(imax, imax-100, -1)))
		# move backwards, relative
		c.seek(-100, 1)
		self.assertEqual([x for x, in c.read(100, 'BACKWARD')], list(range(imax-200, imax-300, -1)))

		# move abs, again
		c.seek(14000)
		self.assertEqual([x for x, in c.read(100)], list(range(14000, 14100)))
		# move forwards, relative
		c.seek(100, 1)
		self.assertEqual([x for x, in c.read(100)], list(range(14200, 14300)))
		# move abs, again
		c.seek(24000)
		self.assertEqual([x for x, in c.read(200)], list(range(24000, 24200)))
		# move to end and then back some
		c.seek(20, 2)
		self.assertEqual([x for x, in c.read(200, 'BACKWARD')], list(range(imax-20, imax-20-200, -1)))

		c.seek(0, 2)
		c.seek(-10, 1)
		r1 = c.read(10)
		c.seek(10, 2)
		self.assertEqual(r1, c.read(10))

	@pg_tmp
	def testSeek(self):
		ps = db.prepare("SELECT i FROM generate_series(0, (2^6)::int - 1) AS g(i)")
		c = ps.declare()

		self.assertEqual(c.seek(4, 'FORWARD'), 4)
		self.assertEqual([x for x, in c.read(10)], list(range(4, 14)))

		self.assertEqual(c.seek(2, 'BACKWARD'), 2)
		self.assertEqual([x for x, in c.read(10)], list(range(12, 22)))

		self.assertEqual(c.seek(-5, 'BACKWARD'), 5)
		self.assertEqual([x for x, in c.read(10)], list(range(27, 37)))

		self.assertEqual(c.seek('ALL'), 27)

	def testScrollBackwards(self):
		# testScroll again, but backwards this time.
		self.testScroll(direction = False)

	@pg_tmp
	def testWithHold(self):
		with db.xact():
			ps = db.prepare("SELECT 1")
			c = ps.declare()
			cid = c.cursor_id
		self.assertEqual(c.read()[0][0], 1)
		# make sure it's not cheating
		self.assertEqual(c.cursor_id, cid)
		# check grabs beyond the default chunksize.
		with db.xact():
			ps = db.prepare("SELECT i FROM generate_series(0, 99) as g(i)")
			c = ps.declare()
			cid = c.cursor_id
		self.assertEqual([x for x, in c.read()], list(range(100)))
		# make sure it's not cheating
		self.assertEqual(c.cursor_id, cid)

	def load_rows(self):
		gs = db.prepare("SELECT i FROM generate_series(1, 10000) AS g(i)")
		self.assertEqual(
			list((x[0] for x in gs.rows())),
			list(range(1, 10001))
		)
		# exercise ``for x in chunks: dst.load_rows(x)``
		with new() as db2:
			db2.execute(
				"""
				CREATE TABLE chunking AS
				SELECT i::text AS t, i::int AS i
				FROM generate_series(1, 10000) g(i);
				"""
			)
			read = db.prepare('select * FROM chunking').rows()
			write = db2.prepare('insert into chunking values ($1, $2)').load_rows
			with db2.xact():
				write(read)
			del read, write

			self.assertEqual(
				db.prepare('select count(*) FROM chunking').first(),
				20000
			)
			self.assertEqual(
				db.prepare('select count(DISTINCT i) FROM chunking').first(),
				10000
			)
		db.execute('DROP TABLE chunking')

	@pg_tmp
	def testLoadRows(self):
		self.load_rows()

	@pg_tmp
	def testLoadRowsInXact(self):
		with db.xact():
			self.load_rows()

	def load_chunks(self):
		gs = db.prepare("SELECT i FROM generate_series(1, 10000) AS g(i)")
		self.assertEqual(
			list((x[0] for x in chain.from_iterable(gs.chunks()))),
			list(range(1, 10001))
		)
		# exercise ``for x in chunks: dst.load_chunks(x)``
		with new() as db2:
			db2.execute(
				"""
				CREATE TABLE chunking AS
				SELECT i::text AS t, i::int AS i
				FROM generate_series(1, 10000) g(i);
				"""
			)
			read = db.prepare('select * FROM chunking').chunks()
			write = db2.prepare('insert into chunking values ($1, $2)').load_chunks
			with db2.xact():
				write(read)
			del read, write

			self.assertEqual(
				db.prepare('select count(*) FROM chunking').first(),
				20000
			)
			self.assertEqual(
				db.prepare('select count(DISTINCT i) FROM chunking').first(),
				10000
			)
		db.execute('DROP TABLE chunking')

	@pg_tmp
	def testLoadChunks(self):
		self.load_chunks()

	@pg_tmp
	def testLoadChunkInXact(self):
		with db.xact():
			self.load_chunks()

	@pg_tmp
	def testSimpleDML(self):
		db.execute("CREATE TEMP TABLE emp(emp_name text, emp_age int)")
		try:
			mkemp = db.prepare("INSERT INTO emp VALUES ($1, $2)")
			del_all_emp = db.prepare("DELETE FROM emp")
			command, count = mkemp('john', 35)
			self.assertEqual(command, 'INSERT')
			self.assertEqual(count, 1)
			command, count = mkemp('jane', 31)
			self.assertEqual(command, 'INSERT')
			self.assertEqual(count, 1)
			command, count = del_all_emp()
			self.assertEqual(command, 'DELETE')
			self.assertEqual(count, 2)
		finally:
			db.execute("DROP TABLE emp")

	def dml(self):
		db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = db.prepare("DELETE FROM t")
			update_t = db.prepare("UPDATE t SET i = $2 WHERE i = $1")
			self.assertEqual(insert_t(1)[1], 1)
			self.assertEqual(delete_t(1)[1], 1)
			self.assertEqual(insert_t(2)[1], 1)
			self.assertEqual(insert_t(2)[1], 1)
			self.assertEqual(delete_t(2)[1], 2)

			self.assertEqual(insert_t(3)[1], 1)
			self.assertEqual(insert_t(3)[1], 1)
			self.assertEqual(insert_t(3)[1], 1)
			self.assertEqual(delete_all_t()[1], 3)

			self.assertEqual(update_t(1, 2)[1], 0)
			self.assertEqual(insert_t(1)[1], 1)
			self.assertEqual(update_t(1, 2)[1], 1)
			self.assertEqual(delete_t(1)[1], 0)
			self.assertEqual(delete_t(2)[1], 1)
		finally:
			db.execute("DROP TABLE t")

	@pg_tmp
	def testDML(self):
		self.dml()

	@pg_tmp
	def testDMLInXact(self):
		with db.xact():
			self.dml()

	def batch_dml(self):
		db.execute("CREATE TEMP TABLE t(i int)")
		try:
			insert_t = db.prepare("INSERT INTO t VALUES ($1)")
			delete_t = db.prepare("DELETE FROM t WHERE i = $1")
			delete_all_t = db.prepare("DELETE FROM t")
			update_t = db.prepare("UPDATE t SET i = $2 WHERE i = $1")
			mset = (
				(2,), (2,), (3,), (4,), (5,),
			)
			insert_t.load_rows(mset)
			content = db.prepare("SELECT * FROM t ORDER BY 1 ASC")
			self.assertEqual(mset, tuple(content()))
		finally:
			db.execute("DROP TABLE t")

	@pg_tmp
	def testBatchDML(self):
		self.batch_dml()

	@pg_tmp
	def testBatchDMLInXact(self):
		with db.xact():
			self.batch_dml()

	@pg_tmp
	def testTypes(self):
		'test basic object I/O--input must equal output'
		for (typname, sample_data) in type_samples:
			pb = db.prepare(
				"SELECT $1::" + typname
			)
			for sample in sample_data:
				rsample = list(pb.rows(sample))[0][0]
				if isinstance(rsample, pg_types.Array):
					rsample = rsample.nest()
				self.assertTrue(
					rsample == sample,
					"failed to return %s object data as-is; gave %r, received %r" %(
						typname, sample, rsample
					)
				)

	@pg_tmp
	def testDomainSupport(self):
		'test domain type I/O'

		db.execute('CREATE DOMAIN int_t AS int')
		db.execute('CREATE DOMAIN int_t_2 AS int_t')
		db.execute('CREATE TYPE tt AS (a int_t, b int_t_2)')

		samples = {
			'int_t': [10],
			'int_t_2': [11],
			'tt': [(12, 13)]
		}

		for (typname, sample_data) in samples.items():
			pb = db.prepare(
				"SELECT $1::" + typname
			)
			for sample in sample_data:
				rsample = list(pb.rows(sample))[0][0]
				if isinstance(rsample, pg_types.Array):
					rsample = rsample.nest()
				self.assertTrue(
					rsample == sample,
					"failed to return %s object data as-is; gave %r, received %r" %(
						typname, sample, rsample
					)
				)

	@pg_tmp
	def testAnonymousRecord(self):
		'test anonymous record unpacking'

		db.execute('CREATE TYPE tar_t AS (a int, b int)')

		tests = {
			"SELECT (1::int, '2'::text, '2012-01-01 18:00 UTC'::timestamptz)":
				(1, '2', datetime.datetime(2012, 1, 1, 18, 0, tzinfo=FixedOffset(0))),

			"SELECT (1::int, '2'::text, (3::int, '4'::text))":
				(1, '2', (3, '4')),

			"SELECT (i::int, (i + 1, i + 2)::tar_t) FROM generate_series(1, 10) as i":
				(1, (2, 3)),

			"SELECT (1::int, ARRAY[(2, 3), (3, 4)])":
				(1, pg_types.Array([(2, 3), (3, 4)]))
		}

		for qry, expected in tests.items():
			pb = db.prepare(qry)
			result = next(iter(pb.rows()))[0]
			self.assertEqual(result, expected)

	def check_xml(self):
		try:
			xml = db.prepare('select $1::xml')
			textxml = db.prepare('select $1::text::xml')
			r = textxml.first('<foo/>')
		except (pg_exc.FeatureError, pg_exc.UndefinedObjectError):
			# XML is not available.
			return
		foo = etree.XML('<foo/>')
		bar = etree.XML('<bar/>')
		if hasattr(etree, 'tostringlist'):
			# 3.2
			def tostr(x):
				return etree.tostring(x, encoding='utf-8')
		else:
			# 3.1 compat
			tostr = etree.tostring
		self.assertEqual(tostr(xml.first(foo)), tostr(foo))
		self.assertEqual(tostr(xml.first(bar)), tostr(bar))
		self.assertEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		self.assertEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		self.assertEqual(tostr(xml.first(etree.XML('<foo/>'))), tostr(foo))
		self.assertEqual(tostr(textxml.first('<foo/>')), tostr(foo))
		# test fragments
		self.assertEqual(
			tuple(
				tostr(x) for x in xml.first('<foo/><bar/>')
			), (tostr(foo), tostr(bar))
		)
		self.assertEqual(
			tuple(
				tostr(x) for x in textxml.first('<foo/><bar/>')
			),
			(tostr(foo), tostr(bar))
		)
		# mixed text and etree.
		self.assertEqual(
			tuple(
				tostr(x) for x in xml.first((
					'<foo/>', bar,
				))
			),
			(tostr(foo), tostr(bar))
		)
		self.assertEqual(
			tuple(
				tostr(x) for x in xml.first((
					'<foo/>', bar,
				))
			),
			(tostr(foo), tostr(bar))
		)

	@pg_tmp
	def testXML(self):
		self.check_xml()

	@pg_tmp
	def testXML_ascii(self):
		# check a non-utf8 encoding (3.2 and up)
		db.settings['client_encoding'] = 'sql_ascii'
		self.check_xml()

	@pg_tmp
	def testXML_utf8(self):
		# in 3.2 we always serialize at utf-8, so check that
		# that path is being ran by forcing the client_encoding to utf8.
		db.settings['client_encoding'] = 'utf8'
		self.check_xml()

	@pg_tmp
	def testUUID(self):
		# doesn't exist in all versions supported by py-postgresql.
		has_uuid = db.prepare(
			"select true from pg_type where lower(typname) = 'uuid'").first()
		if has_uuid:
			ps = db.prepare('select $1::uuid').first
			x = uuid.uuid1()
			self.assertEqual(ps(x), x)

	def _infinity_test(self, typname, inf, neg):
		ps = db.prepare('SELECT $1::' + typname).first
		val = ps('infinity')
		self.assertEqual(val, inf)
		val = ps('-infinity')
		self.assertEqual(val, neg)
		val = ps(inf)
		self.assertEqual(val, inf)
		val = ps(neg)
		self.assertEqual(val, neg)
		ps = db.prepare('SELECT $1::' + typname + '::text').first
		self.assertEqual(ps('infinity'), 'infinity')
		self.assertEqual(ps('-infinity'), '-infinity')

	@pg_tmp
	def testInfinity_stdlib_datetime(self):
		self._infinity_test("timestamptz", infinity_datetime, negative_infinity_datetime)
		self._infinity_test("timestamp", infinity_datetime, negative_infinity_datetime)

	@pg_tmp
	def testInfinity_stdlib_date(self):
		try:
			db.prepare("SELECT 'infinity'::date")()
			self._infinity_test('date', infinity_date, negative_infinity_date)
		except:
			pass

	@pg_tmp
	def testTypeIOError(self):
		original = dict(db.typio._cache)
		ps = db.prepare('SELECT $1::numeric')
		self.assertRaises(pg_exc.ParameterError, ps, 'foo')
		try:
			db.execute('CREATE type test_tuple_error AS (n numeric);')
			ps = db.prepare('SELECT $1::test_tuple_error AS the_column')
			self.assertRaises(pg_exc.ParameterError, ps, ('foo',))
			try:
				ps(('foo',))
			except pg_exc.ParameterError as err:
				# 'foo' is not a valid Decimal.
				# Expecting a double TupleError here, one from the composite pack
				# and one from the row pack.
				self.assertTrue(isinstance(err.__cause__, pg_exc.CompositeError))
				self.assertEqual(int(err.details['position']), 0)
				# attribute number that the failure occurred on
				self.assertEqual(int(err.__cause__.details['position']), 0)
			else:
				self.fail("failed to raise TupleError")

			# testing tuple error reception is a bit more difficult.
			# to do this, we need to immitate failure as we can't rely that any
			# causable failure will always exist.
			class ThisError(Exception):
				pass
			def raise_ThisError(arg):
				raise ThisError(arg)
			pack, unpack, typ = db.typio.resolve(pg_types.NUMERICOID)
			# remove any existing knowledge about "test_tuple_error"
			db.typio._cache = original
			db.typio._cache[pg_types.NUMERICOID] = (pack, raise_ThisError, typ)
			# Now, numeric_unpack will always raise "ThisError".
			ps = db.prepare('SELECT $1::numeric as col')
			self.assertRaises(
				pg_exc.ColumnError, ps, decimal.Decimal("101")
			)
			try:
				ps(decimal.Decimal("101"))
			except pg_exc.ColumnError as err:
				self.assertTrue(isinstance(err.__cause__, ThisError))
				# might be too inquisitive....
				self.assertEqual(int(err.details['position']), 0)
				self.assertTrue('NUMERIC' in err.message)
				self.assertTrue('col' in err.message)
			else:
				self.fail("failed to raise TupleError from reception")
			ps = db.prepare('SELECT $1::test_tuple_error AS tte')
			try:
				ps((decimal.Decimal("101"),))
			except pg_exc.ColumnError as err:
				self.assertTrue(isinstance(err.__cause__, pg_exc.CompositeError))
				self.assertTrue(isinstance(err.__cause__.__cause__, ThisError))
				# might be too inquisitive....
				self.assertEqual(int(err.details['position']), 0)
				self.assertEqual(int(err.__cause__.details['position']), 0)
				self.assertTrue('test_tuple_error' in err.message)
			else:
				self.fail("failed to raise TupleError from reception")
		finally:
			db.execute('drop type test_tuple_error;')

	@pg_tmp
	def testSyntaxError(self):
		try:
			db.prepare("SELEKT 1")()
		except pg_exc.SyntaxError:
			return
		self.fail("SyntaxError was not raised")

	@pg_tmp
	def testSchemaNameError(self):
		try:
			db.prepare("SELECT * FROM sdkfldasjfdskljZknvson.foo")()
		except pg_exc.SchemaNameError:
			return
		self.fail("SchemaNameError was not raised")

	@pg_tmp
	def testUndefinedTableError(self):
		try:
			db.prepare("SELECT * FROM public.lkansdkvsndlvksdvnlsdkvnsdlvk")()
		except pg_exc.UndefinedTableError:
			return
		self.fail("UndefinedTableError was not raised")

	@pg_tmp
	def testUndefinedColumnError(self):
		try:
			db.prepare("SELECT x____ysldvndsnkv FROM information_schema.tables")()
		except pg_exc.UndefinedColumnError:
			return
		self.fail("UndefinedColumnError was not raised")

	@pg_tmp
	def testSEARVError_avgInWhere(self):
		try:
			db.prepare("SELECT 1 WHERE avg(1) = 1")()
		except pg_exc.SEARVError:
			return
		self.fail("SEARVError was not raised")

	@pg_tmp
	def testSEARVError_groupByAgg(self):
		try:
			db.prepare("SELECT 1 GROUP BY avg(1)")()
		except pg_exc.SEARVError:
			return
		self.fail("SEARVError was not raised")

	@pg_tmp
	def testTypeMismatchError(self):
		try:
			db.prepare("SELECT 1 WHERE 1")()
		except pg_exc.TypeMismatchError:
			return
		self.fail("TypeMismatchError was not raised")

	@pg_tmp
	def testUndefinedObjectError(self):
		try:
			self.assertRaises(
				pg_exc.UndefinedObjectError,
				db.prepare, "CREATE TABLE lksvdnvsdlksnv(i intt___t)"
			)
		except:
			# newer versions throw the exception on execution
			self.assertRaises(
				pg_exc.UndefinedObjectError,
				db.prepare("CREATE TABLE lksvdnvsdlksnv(i intt___t)")
			)

	@pg_tmp
	def testZeroDivisionError(self):
		self.assertRaises(
			pg_exc.ZeroDivisionError,
			db.prepare("SELECT 1/i FROM (select 0 as i) AS g(i)").first,
		)

	@pg_tmp
	def testTransactionCommit(self):
		with db.xact():
			db.execute("CREATE TEMP TABLE withfoo(i int)")
		db.prepare("SELECT * FROM withfoo")

		db.execute("DROP TABLE withfoo")
		self.assertRaises(
			pg_exc.UndefinedTableError,
			db.execute, "SELECT * FROM withfoo"
		)

	@pg_tmp
	def testTransactionAbort(self):
		class SomeError(Exception):
			pass
		try:
			with db.xact():
				db.execute("CREATE TABLE withfoo (i int)")
				raise SomeError
		except SomeError:
			pass
		self.assertRaises(
			pg_exc.UndefinedTableError,
			db.execute, "SELECT * FROM withfoo"
		)

	@pg_tmp
	def testSerializeable(self):
		with new() as db2:
			db2.execute("create table some_darn_table (i int);")
			try:
				with db.xact(isolation = 'serializable'):
					db.execute('insert into some_darn_table values (123);')
					# db2 is in autocommit..
					db2.execute('insert into some_darn_table values (321);')
					self.assertNotEqual(
						list(db.prepare('select * from some_darn_table')),
						list(db2.prepare('select * from some_darn_table')),
					)
			finally:
				# cleanup
				db2.execute("drop table some_darn_table;")

	@pg_tmp
	def testReadOnly(self):
		class something(Exception):
			pass
		try:
			with db.xact(mode = 'read only'):
				self.assertRaises(
					pg_exc.ReadOnlyTransactionError,
					db.execute,
					"create table ieeee(i int)"
				)
				raise something("yeah, it raised.")
			self.fail("should have been passed by exception")
		except something:
			pass

	@pg_tmp
	def testFailedTransactionBlock(self):
		try:
			with db.xact():
				try:
					db.execute("selekt 1;")
				except pg_exc.SyntaxError:
					pass
			self.fail("__exit__ didn't identify failed transaction")
		except pg_exc.InFailedTransactionError as err:
			self.assertEqual(err.source, 'CLIENT')

	@pg_tmp
	def testFailedSubtransactionBlock(self):
		with db.xact():
			try:
				with db.xact():
					try:
						db.execute("selekt 1;")
					except pg_exc.SyntaxError:
						pass
				self.fail("__exit__ didn't identify failed transaction")
			except pg_exc.InFailedTransactionError as err:
				# driver should have released/aborted instead
				self.assertEqual(err.source, 'CLIENT')

	@pg_tmp
	def testSuccessfulSubtransactionBlock(self):
		with db.xact():
			with db.xact():
				db.execute("create temp table subxact_sx1(i int);")
				with db.xact():
					db.execute("create temp table subxact_sx2(i int);")
					# And, because I'm paranoid.
					# The following block is used to make sure
					# that savepoints are actually being set.
					try:
						with db.xact():
							db.execute("selekt 1")
					except pg_exc.SyntaxError:
						# Just in case the xact() aren't doing anything.
						pass
			with db.xact():
				db.execute("create temp table subxact_sx3(i int);")
		# if it can't drop these tables, it didn't manage the subxacts
		# properly.
		db.execute("drop table subxact_sx1")
		db.execute("drop table subxact_sx2")
		db.execute("drop table subxact_sx3")

	@pg_tmp
	def testReleasedSavepoint(self):
		# validate that the rolled back savepoint is released as well.
		x = None
		with db.xact():
			try:
				with db.xact():
					try:
						with db.xact() as x:
							db.execute("selekt 1")
					except pg_exc.SyntaxError:
						db.execute('RELEASE "xact(' + hex(id(x)) + ')"')
			except pg_exc.InvalidSavepointSpecificationError as e:
				pass
			else:
				self.fail("InvalidSavepointSpecificationError not raised")

	@pg_tmp
	def testCloseInSubTransactionBlock(self):
		try:
			with db.xact():
				db.close()
			self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
		except pg_exc.ConnectionDoesNotExistError:
			pass

	@pg_tmp
	def testCloseInSubTransactionBlock(self):
		try:
			with db.xact():
				with db.xact():
					db.close()
				self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
			self.fail("transaction __exit__ didn't identify cause ConnectionDoesNotExistError")
		except pg_exc.ConnectionDoesNotExistError:
			pass

	@pg_tmp
	def testSettingsCM(self):
		orig = db.settings['search_path']
		with db.settings(search_path='public'):
			self.assertEqual(db.settings['search_path'], 'public')
		self.assertEqual(db.settings['search_path'], orig)

	@pg_tmp
	def testSettingsReset(self):
		# <3 search_path
		del db.settings['search_path']
		cur = db.settings['search_path']
		db.settings['search_path'] = 'pg_catalog'
		del db.settings['search_path']
		self.assertEqual(db.settings['search_path'], cur)

	@pg_tmp
	def testSettingsCount(self):
		self.assertEqual(
			len(db.settings), db.prepare('select count(*) from pg_settings').first()
		)

	@pg_tmp
	def testSettingsGet(self):
		self.assertEqual(
			db.settings['search_path'], db.settings.get('search_path')
		)
		self.assertEqual(None, db.settings.get(' $*0293 vksnd'))

	@pg_tmp
	def testSettingsGetSet(self):
		sub = db.settings.getset(
			('search_path', 'default_statistics_target')
		)
		self.assertEqual(db.settings['search_path'], sub['search_path'])
		self.assertEqual(db.settings['default_statistics_target'], sub['default_statistics_target'])

	@pg_tmp
	def testSettings(self):
		d = dict(db.settings)
		d = dict(db.settings.items())
		k = list(db.settings.keys())
		v = list(db.settings.values())
		self.assertEqual(len(k), len(d))
		self.assertEqual(len(k), len(v))
		for x in k:
			self.assertTrue(d[x] in v)
		all = list(db.settings.getset(k).items())
		all.sort(key=itemgetter(0))
		dall = list(d.items())
		dall.sort(key=itemgetter(0))
		self.assertEqual(dall, all)

	@pg_tmp
	def testDo(self):
		# plpgsql is expected to be available.
		if db.version_info[:2] < (8,5):
			return
		if 'plpgsql' not in db.sys.languages():
			db.execute("CREATE LANGUAGE plpgsql")
		db.do('plpgsql', "BEGIN CREATE TEMP TABLE do_tmp_table(i int, t text); END",)
		self.assertEqual(len(db.prepare("SELECT * FROM do_tmp_table")()), 0)
		db.do('plpgsql', "BEGIN INSERT INTO do_tmp_table VALUES (100, 'foo'); END")
		self.assertEqual(len(db.prepare("SELECT * FROM do_tmp_table")()), 1)

	@pg_tmp
	def testListeningChannels(self):
		db.listen('foo', 'bar')
		self.assertEqual(set(db.listening_channels()), {'foo','bar'})
		db.unlisten('bar')
		db.listen('foo', 'bar')
		self.assertEqual(set(db.listening_channels()), {'foo','bar'})
		db.unlisten('foo', 'bar')
		self.assertEqual(set(db.listening_channels()), set())

	@pg_tmp
	def testNotify(self):
		db.listen('foo', 'bar')
		db.listen('foo', 'bar')
		db.notify('foo')
		db.execute('')
		self.assertEqual(db._notifies[0].channel, b'foo')
		self.assertEqual(db._notifies[0].pid, db.backend_id)
		self.assertEqual(db._notifies[0].payload, b'')
		del db._notifies[0]
		db.notify('bar')
		db.execute('')
		self.assertEqual(db._notifies[0].channel, b'bar')
		self.assertEqual(db._notifies[0].pid, db.backend_id)
		self.assertEqual(db._notifies[0].payload, b'')
		del db._notifies[0]
		db.unlisten('foo')
		db.notify('foo')
		db.execute('')
		self.assertEqual(db._notifies, [])
		# Invoke an error to show that listen() is all or none.
		self.assertRaises(Exception, db.listen, 'doesntexist', 'x'*64)
		self.assertTrue('doesntexist' not in db.listening_channels())

	@pg_tmp
	def testPayloads(self):
		if db.version_info[:2] >= (9,0):
			db.listen('foo')
			db.notify(foo = 'bar')
			self.assertEqual(('foo', 'bar', db.backend_id), list(db.iternotifies(0))[0])
			db.notify(('foo', 'barred'))
			self.assertEqual(('foo', 'barred', db.backend_id), list(db.iternotifies(0))[0])
			# mixed
			db.notify(('foo', 'barred'), 'foo', ('foo', 'bleh'), foo = 'kw')
			self.assertEqual([
					('foo', 'barred', db.backend_id),
					('foo', '', db.backend_id),
					('foo', 'bleh', db.backend_id),
					# Keywords are appened.
					('foo', 'kw', db.backend_id),
				], list(db.iternotifies(0))
			)
			# multiple keywords
			expect = [
				('foo', 'meh', db.backend_id),
				('bar', 'foo', db.backend_id),
			]
			rexpect = list(reversed(expect))
			db.listen('bar')
			db.notify(foo = 'meh', bar = 'foo')
			self.assertTrue(list(db.iternotifies(0)) in [expect, rexpect])

	@pg_tmp
	def testMessageHook(self):
		create = db.prepare('CREATE TEMP TABLE msghook (i INT PRIMARY KEY)')
		drop = db.prepare('DROP TABLE msghook')
		parts = [
			create,
			db,
			db.connector,
			db.connector.driver,
		]
		notices = []
		def add(x):
			notices.append(x)
			# inhibit
			return True
		with db.xact():
			db.settings['client_min_messages'] = 'NOTICE'
			# test an installed msghook at each level
			for x in parts:
				x.msghook = add
				create()
				del x.msghook
				drop()
		self.assertEqual(len(notices), len(parts))
		last = None
		for x in notices:
			if last is None:
				last = x
				continue
			self.assertTrue(x.isconsistent(last))
			last = x

	@pg_tmp
	def testRowTypeFactory(self):
		from ..types.namedtuple import NamedTupleFactory
		db.typio.RowTypeFactory = NamedTupleFactory
		ps = prepare('select 1 as foo, 2 as bar')
		first_results = ps.first()
		self.assertEqual(first_results.foo, 1)
		self.assertEqual(first_results.bar, 2)

		call_results = ps()[0]
		self.assertEqual(call_results.foo, 1)
		self.assertEqual(call_results.bar, 2)

		declare_results = ps.declare().read(1)[0]
		self.assertEqual(declare_results.foo, 1)
		self.assertEqual(declare_results.bar, 2)

		sqlexec('create type rtf AS (foo int, bar int)')
		ps = prepare('select ROW(1, 2)::rtf')
		composite_results = ps.first()
		self.assertEqual(composite_results.foo, 1)
		self.assertEqual(composite_results.bar, 2)

	@pg_tmp
	def testNamedTuples(self):
		from ..types.namedtuple import namedtuples
		ps = namedtuples(prepare('select 1 as foo, 2 as bar, $1::text as param'))
		r = list(ps("hello"))[0]
		self.assertEqual(r[0], 1)
		self.assertEqual(r.foo, 1)
		self.assertEqual(r[1], 2)
		self.assertEqual(r.bar, 2)
		self.assertEqual(r[2], "hello")
		self.assertEqual(r.param, "hello")

	@pg_tmp
	def testBadFD(self):
		db.pq.socket.close()
		# bad fd now.
		self.assertRaises(
			pg_exc.ConnectionFailureError,
			sqlexec, "SELECT 1"
		)
		self.assertTrue(issubclass(pg_exc.ConnectionFailureError, pg_exc.Disconnection))

	@pg_tmp
	def testAdminTerminated(self):
		with new() as killer:
			if killer.version_info[:2] <= (9,1):
				killer.sys.terminate_backends()
			else:
				killer.sys.terminate_backends_92()

		self.assertRaises(
			pg_exc.AdminShutdownError,
			sqlexec, "SELECT 1",
		)
		self.assertTrue(issubclass(pg_exc.AdminShutdownError, pg_exc.Disconnection))

	@pg_tmp
	def testQuery(self):
		self.assertEqual(db.query('select 1'), [(1,)])
		self.assertEqual(db.query.first('select 1'), 1)
		self.assertEqual(next(db.query.column('select 1')), 1)
		self.assertEqual(next(db.query.rows('select 1')), (1,))
		self.assertEqual(db.query.declare('select 1').read(), [(1,)])

		self.assertEqual(db.query('select $1::int', 1), [(1,)])
		self.assertEqual(db.query.first('select $1::int', 1), 1)
		self.assertEqual(next(db.query.column('select $1::int', 1)), 1)
		self.assertEqual(next(db.query.rows('select $1::int', 1)), (1,))
		self.assertEqual(db.query.declare('select $1::int', 1).read(), [(1,)])

		self.assertEqual(db.query.load_rows('select $1::int', [[1]]), None)
		self.assertEqual(db.query.load_chunks('select $1::int', [[[1]]]), None)

class test_typio(unittest.TestCase):
	@pg_tmp
	def testIdentify(self):
		# It just exercises the code path.
		db.typio.identify(contrib_hstore = 'pg_catalog.reltime')

	@pg_tmp
	def testArrayNulls(self):
		try:
			sqlexec('SELECT ARRAY[1,NULL]::int[]')
		except Exception:
			# unsupported here
			return
		inta = prepare('select $1::int[]').first
		texta = prepare('select $1::text[]').first
		self.assertEqual(inta([1,2,None]), [1,2,None])
		self.assertEqual(texta(["foo",None,"bar"]), ["foo",None,"bar"])

if __name__ == '__main__':
	unittest.main()
