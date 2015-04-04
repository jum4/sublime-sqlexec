##
# .test.test_pgpassfile
##
import unittest
from .. import pgpassfile as client_pgpass
from io import StringIO

passfile_sample = """
# host:1111:dbname:user:password1
host:1111:dbname:user:password1
*:1111:dbname:user:password2
*:*:dbname:user:password3

# Comment

*:*:*:user:password4
*:*:*:usern:password4.5
*:*:*:*:password5
"""

passfile_sample_map = {
	('user', 'host', '1111', 'dbname') : 'password1',
	('user', 'host', '1111', 'dbname') : 'password1',
	('user', 'foo', '1111', 'dbname') : 'password2',
	('user', 'foo', '4321', 'dbname') : 'password3',
	('user', 'foo', '4321', 'db,name') : 'password4',

	('uuser', 'foo', '4321', 'db,name') : 'password5',
	('usern', 'foo', '4321', 'db,name') : 'password4.5',
	('foo', 'bar', '19231', 'somedbn') : 'password5',
}

difficult_passfile_sample = r"""
host\\:1111:db\:name:u\\ser:word1
*:1111:\:dbname\::\\user\\:pass\:word2
foohost:1111:\:dbname\::\\user\\:pass\:word3
"""

difficult_passfile_sample_map = {
	('u\\ser','host\\','1111','db:name') : 'word1',
	('\\user\\','somehost','1111',':dbname:') : 'pass:word2',
	('\\user\\','someotherhost','1111',':dbname:') : 'pass:word2',
	# More specific, but comes after '*'
	('\\user\\','foohost','1111',':dbname:') : 'pass:word2',
	('','','','') : None,
}

class test_pgpass(unittest.TestCase):
	def runTest(self):
		sample1 = client_pgpass.parse(StringIO(passfile_sample))
		sample2 = client_pgpass.parse(StringIO(difficult_passfile_sample))

		for k, pw in passfile_sample_map.items():
			lpw = client_pgpass.lookup_password(sample1, k)
			self.assertEqual(lpw, pw,
				"password lookup incongruity, expecting %r got %r with %r"
				" in \n%s" %(
					pw, lpw, k, passfile_sample
				)
			)

		for k, pw in difficult_passfile_sample_map.items():
			lpw = client_pgpass.lookup_password(sample2, k)
			self.assertEqual(lpw, pw,
				"password lookup incongruity, expecting %r got %r with %r"
				" in \n%s" %(
					pw, lpw, k, difficult_passfile_sample
				)
			)

if __name__ == '__main__':
	unittest.main()
