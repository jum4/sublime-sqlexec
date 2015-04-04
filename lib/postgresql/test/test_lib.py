##
# .test.test_lib - test the .lib package
##
import sys
import os
import unittest
import tempfile

from .. import exceptions as pg_exc
from .. import lib as pg_lib
from .. import sys as pg_sys
from ..temporal import pg_tmp

ilf = """
preface

[sym]
select 1
[sym_ref]
*[sym]
[sym_ref_trail]
*[sym] WHERE FALSE
[sym_first::first]
select 1


[sym_rows::rows]
select 1

[sym_chunks::chunks]
select 1

[sym_declare::declare]
select 1

[sym_const:const:first]
select 1
[sym_const_rows:const:rows]
select 1
[sym_const_chunks:const:chunks]
select 1
[sym_const_column:const:column]
select 1
[sym_const_ddl:const:]
create temp table sym_const_dll (i int);

[sym_preload:preload:first]
select 1

[sym_proc:proc]
test_ilf_proc(int)

[sym_srf_proc:proc]
test_ilf_srf_proc(int)

[&sym_reference]
SELECT 'SELECT 1';

[&sym_reference_params]
SELECT 'SELECT ' || $1::text;

[&sym_reference_first::first]
SELECT 'SELECT 1::int4';

[&sym_reference_const:const:first]
SELECT 'SELECT 1::int4';

[&sym_reference_proc:proc]
SELECT 'test_ilf_proc(int)'::text
"""

class test_lib(unittest.TestCase):
	# NOTE: Module libraries are implicitly tested
	# in postgresql.test.test_driver; much functionality
	# depends on the `sys` library.
	def _testILF(self, lib):
		self.assertTrue('preface' in lib.preface)
		db.execute("CREATE OR REPLACE FUNCTION test_ilf_proc(int) RETURNS int language sql as 'select $1';")
		db.execute("CREATE OR REPLACE FUNCTION test_ilf_srf_proc(int) RETURNS SETOF int language sql as 'select $1';")
		b = pg_lib.Binding(db, lib)
		self.assertEqual(b.sym_ref(), [(1,)])
		self.assertEqual(b.sym_ref_trail(), [])
		self.assertEqual(b.sym(), [(1,)])
		self.assertEqual(b.sym_first(), 1)
		self.assertEqual(list(b.sym_rows()), [(1,)])
		self.assertEqual([list(x) for x in b.sym_chunks()], [[(1,)]])
		c = b.sym_declare()
		self.assertEqual(c.read(), [(1,)])
		c.seek(0)
		self.assertEqual(c.read(), [(1,)])
		self.assertEqual(b.sym_const, 1)
		self.assertEqual(b.sym_const_column, [1])
		self.assertEqual(b.sym_const_rows, [(1,)])
		self.assertEqual(b.sym_const_chunks, [[(1,)]])
		self.assertEqual(b.sym_const_ddl, ('CREATE TABLE', None))
		self.assertEqual(b.sym_preload(), 1)
		# now stored procs
		self.assertEqual(b.sym_proc(2,), 2)
		self.assertEqual(list(b.sym_srf_proc(2,)), [2])
		self.assertRaises(AttributeError, getattr, b, 'LIES')
		# reference symbols
		self.assertEqual(b.sym_reference()(), [(1,)])
		self.assertEqual(b.sym_reference_params('1::int')(), [(1,)])
		self.assertEqual(b.sym_reference_params("'foo'::text")(), [('foo',)])
		self.assertEqual(b.sym_reference_first()(), 1)
		self.assertEqual(b.sym_reference_const(), 1)
		self.assertEqual(b.sym_reference_proc()(2,), 2)

	@pg_tmp
	def testILF_from_lines(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		self._testILF(lib)

	@pg_tmp
	def testILF_from_file(self):
		f = tempfile.NamedTemporaryFile(
			delete = False, mode = 'w', encoding = 'utf-8'
		) 
		n = f.name
		try:
			f.write(ilf)
			f.flush()
			f.seek(0)
			lib = pg_lib.ILF.open(n, encoding = 'utf-8')
			self._testILF(lib)
			f.close()
		finally:
			# so annoying...
			os.unlink(n)

	@pg_tmp
	def testLoad(self):
		# gotta test it in the cwd...
		pid = os.getpid()
		frag = 'temp' + str(pid)
		fn = 'lib' + frag + '.sql'
		try:
			with open(fn, 'w') as f:
				f.write("[foo]\nSELECT 1")
			pg_sys.libpath.insert(0, os.path.curdir)
			l = pg_lib.load(frag)
			b = pg_lib.Binding(db, l)
			self.assertEqual(b.foo(), [(1,)])
		finally:
			os.remove(fn)
	
	@pg_tmp
	def testCategory(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		# XXX: evil, careful..
		lib._name = 'name'
		c = pg_lib.Category(lib)
		c(db)
		self.assertEqual(db.name.sym_first(), 1)
		c = pg_lib.Category(renamed = lib)
		c(db)
		self.assertEqual(db.renamed.sym_first(), 1)

	@pg_tmp
	def testCategoryAliases(self):
		lib = pg_lib.ILF.from_lines([l + '\n' for l in ilf.splitlines()])
		# XXX: evil, careful..
		lib._name = 'name'
		c = pg_lib.Category(lib, renamed = lib)
		c(db)
		self.assertEqual(db.name.sym_first(), 1)
		self.assertEqual(db.renamed.sym_first(), 1)

if __name__ == '__main__':
	unittest.main()
