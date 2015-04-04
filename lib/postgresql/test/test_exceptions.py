##
# .test.test_exceptions
##
import unittest
import postgresql.exceptions as pg_exc

class test_exceptions(unittest.TestCase):
	def test_pg_code_lookup(self):
		# in 8.4, pg started using the SQL defined error code for limits
		# Users *will* get whatever code PG sends, but it's important
		# that they have some way to abstract it. many-to-one map ftw.
		self.assertEqual(
			pg_exc.ErrorLookup('22020'), pg_exc.LimitValueError
		)

	def test_error_lookup(self):
		# An error code that doesn't exist yields pg_exc.Error
		self.assertEqual(
			pg_exc.ErrorLookup('00000'), pg_exc.Error
		)

		self.assertEqual(
			pg_exc.ErrorLookup('XX000'), pg_exc.InternalError
		)
		# check class fallback
		self.assertEqual(
			pg_exc.ErrorLookup('XX444'), pg_exc.InternalError
		)

		# SEARV is a very large class, so there are many
		# sub-"codeclass" exceptions used to group the many
		# SEARV errors. Make sure looking up 42000 actually
		# gives the SEARVError
		self.assertEqual(
			pg_exc.ErrorLookup('42000'), pg_exc.SEARVError
		)
		self.assertEqual(
			pg_exc.ErrorLookup('08P01'), pg_exc.ProtocolError
		)

	def test_warning_lookup(self):
		self.assertEqual(
			pg_exc.WarningLookup('01000'), pg_exc.Warning
		)
		self.assertEqual(
			pg_exc.WarningLookup('02000'), pg_exc.NoDataWarning
		)
		self.assertEqual(
			pg_exc.WarningLookup('01P01'), pg_exc.DeprecationWarning
		)
		self.assertEqual(
			pg_exc.WarningLookup('01888'), pg_exc.Warning
		)

if __name__ == '__main__':
	unittest.main()
