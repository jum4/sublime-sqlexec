##
# .test.test_ssl_connect
##
import sys
import os
import unittest

from .. import exceptions as pg_exc
from .. import driver as pg_driver
from ..driver import dbapi20
from . import test_connect

server_key = """
-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQCy8veVaqL6MZVT8o0j98ggZYfibGwSN4XGC4rfineA2QZhi8t+
zrzfOS10vLXKtgiIpevHeQbDlrqFDPUDowozurg+jfro2L1jzQjZPdgqOUs+YjKh
EO0Ya7NORO7ZgBx8WveXq30k4l8DK41jvpxRyBb9aqNWG4cB7fJqVTwZrwIDAQAB
AoGAJ74URGfheEVoz7MPq4xNMvy5mAzSV51jJV/M4OakscYBR8q/UBNkGQNe2A1N
Jo8VCBwpaCy11txz4jbFd6BPFFykgXleuRvMxoTv1qV0dZZ0X0ESNEAnjoHtjin/
25mxsZTR6ucejHqXD9qE9NvFQ+wLv6Xo5rgDpx0onvgLA3kCQQDn4GeMkCfPZCve
lDUK+TpJnLYupyElZiidoFMITlFo5WoWNJror2W42A5TD9sZ23pGSxw7ypiWIF4f
ukGT5ZSzAkEAxZDwUUhgtoJIK7E9sCJM4AvcjDxGjslbUI/SmQTT+aTNCAmcIRrl
kq3WMkPjxi/QFEdkIpPsV9Kc94oQ/8b9FQJBAKHxRQCTsWoTsNvbsIwAcif1Lfu5
N9oR1i34SeVUJWFYUFY/2SzHSwjkxGRYf5I4idZMIOTVYun+ox4PjDtJrScCQEQ4
RiNrIKok1pLvwuNdFLqQnfl2ns6TTQrGfuwDtMaRV5Mc7mKoDPnXOQ1mT/KRdAJs
nHEsLwIsYbNAY5pOtfkCQDOy2Ffe7Z1YzFZXCTzpcq4mvMOPEUqlIX6hACNJGhgt
1EpruPwqR2PYDOIC4sXCaSogL8YyjI+Jlhm5kEJ4GaU=
-----END RSA PRIVATE KEY-----
"""

server_crt = """
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number:
            a1:02:62:34:22:0d:45:6a
        Signature Algorithm: md5WithRSAEncryption
        Issuer: C=US, ST=Arizona, L=Nowhere, O=ACME Inc, OU=Test Division, CN=test.python.projects.postgresql.org
        Validity
            Not Before: Feb 18 15:52:20 2009 GMT
            Not After : Mar 20 15:52:20 2009 GMT
        Subject: C=US, ST=Arizona, L=Nowhere, O=ACME Inc, OU=Test Division, CN=test.python.projects.postgresql.org
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
            RSA Public Key: (1024 bit)
                Modulus (1024 bit):
                    00:b2:f2:f7:95:6a:a2:fa:31:95:53:f2:8d:23:f7:
                    c8:20:65:87:e2:6c:6c:12:37:85:c6:0b:8a:df:8a:
                    77:80:d9:06:61:8b:cb:7e:ce:bc:df:39:2d:74:bc:
                    b5:ca:b6:08:88:a5:eb:c7:79:06:c3:96:ba:85:0c:
                    f5:03:a3:0a:33:ba:b8:3e:8d:fa:e8:d8:bd:63:cd:
                    08:d9:3d:d8:2a:39:4b:3e:62:32:a1:10:ed:18:6b:
                    b3:4e:44:ee:d9:80:1c:7c:5a:f7:97:ab:7d:24:e2:
                    5f:03:2b:8d:63:be:9c:51:c8:16:fd:6a:a3:56:1b:
                    87:01:ed:f2:6a:55:3c:19:af
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Subject Key Identifier: 
                4B:2F:4F:1A:43:75:43:DC:26:59:89:48:56:73:BB:D0:AA:95:E8:60
            X509v3 Authority Key Identifier: 
                keyid:4B:2F:4F:1A:43:75:43:DC:26:59:89:48:56:73:BB:D0:AA:95:E8:60
                DirName:/C=US/ST=Arizona/L=Nowhere/O=ACME Inc/OU=Test Division/CN=test.python.projects.postgresql.org
                serial:A1:02:62:34:22:0D:45:6A

            X509v3 Basic Constraints: 
                CA:TRUE
    Signature Algorithm: md5WithRSAEncryption
        24:ee:20:0f:b5:86:08:d6:3c:8f:d4:8d:16:fd:ac:e8:49:77:
        86:74:7d:b8:f3:15:51:1d:d8:65:17:5e:a8:58:aa:b0:f6:68:
        45:cb:77:9d:9f:21:81:e3:5e:86:1c:64:31:39:b6:29:5f:f1:
        ec:b1:33:45:1f:0c:54:16:26:11:af:e2:23:1b:a6:03:46:9b:
        0e:63:ce:2c:02:41:26:93:bc:6f:6e:08:7e:95:b7:7a:f9:3a:
        5a:bd:47:4c:92:ce:ea:09:75:de:3d:bb:30:51:a0:c5:f1:5d:
        33:5f:c0:37:75:53:4e:6c:b4:3b:b1:a5:1b:fd:59:19:07:18:
        22:6a
-----BEGIN CERTIFICATE-----
MIIDhzCCAvCgAwIBAgIJAKECYjQiDUVqMA0GCSqGSIb3DQEBBAUAMIGKMQswCQYD
VQQGEwJVUzEQMA4GA1UECBMHQXJpem9uYTEQMA4GA1UEBxMHTm93aGVyZTERMA8G
A1UEChMIQUNNRSBJbmMxFjAUBgNVBAsTDVRlc3QgRGl2aXNpb24xLDAqBgNVBAMT
I3Rlc3QucHl0aG9uLnByb2plY3RzLnBvc3RncmVzcWwub3JnMB4XDTA5MDIxODE1
NTIyMFoXDTA5MDMyMDE1NTIyMFowgYoxCzAJBgNVBAYTAlVTMRAwDgYDVQQIEwdB
cml6b25hMRAwDgYDVQQHEwdOb3doZXJlMREwDwYDVQQKEwhBQ01FIEluYzEWMBQG
A1UECxMNVGVzdCBEaXZpc2lvbjEsMCoGA1UEAxMjdGVzdC5weXRob24ucHJvamVj
dHMucG9zdGdyZXNxbC5vcmcwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBALLy
95VqovoxlVPyjSP3yCBlh+JsbBI3hcYLit+Kd4DZBmGLy37OvN85LXS8tcq2CIil
68d5BsOWuoUM9QOjCjO6uD6N+ujYvWPNCNk92Co5Sz5iMqEQ7Rhrs05E7tmAHHxa
95erfSTiXwMrjWO+nFHIFv1qo1YbhwHt8mpVPBmvAgMBAAGjgfIwge8wHQYDVR0O
BBYEFEsvTxpDdUPcJlmJSFZzu9CqlehgMIG/BgNVHSMEgbcwgbSAFEsvTxpDdUPc
JlmJSFZzu9CqlehgoYGQpIGNMIGKMQswCQYDVQQGEwJVUzEQMA4GA1UECBMHQXJp
em9uYTEQMA4GA1UEBxMHTm93aGVyZTERMA8GA1UEChMIQUNNRSBJbmMxFjAUBgNV
BAsTDVRlc3QgRGl2aXNpb24xLDAqBgNVBAMTI3Rlc3QucHl0aG9uLnByb2plY3Rz
LnBvc3RncmVzcWwub3JnggkAoQJiNCINRWowDAYDVR0TBAUwAwEB/zANBgkqhkiG
9w0BAQQFAAOBgQAk7iAPtYYI1jyP1I0W/azoSXeGdH248xVRHdhlF16oWKqw9mhF
y3ednyGB416GHGQxObYpX/HssTNFHwxUFiYRr+IjG6YDRpsOY84sAkEmk7xvbgh+
lbd6+TpavUdMks7qCXXePbswUaDF8V0zX8A3dVNObLQ7saUb/VkZBxgiag==
-----END CERTIFICATE-----
"""

class test_ssl_connect(test_connect.test_connect):
	"""
	Run test_connect, but with SSL.
	"""
	params = {'sslmode' : 'require'}
	cluster_path_suffix = '_test_ssl_connect'

	def configure_cluster(self):
		super().configure_cluster()
		self.cluster.settings['ssl'] = 'on'
		with open(self.cluster.hba_file, 'a') as hba:
			hba.writelines([
				# nossl user
				"\n",
				"hostnossl test nossl 0::0/0 trust\n",
				"hostnossl test nossl 0.0.0.0/0 trust\n",
				# ssl-only user
				"hostssl test sslonly 0.0.0.0/0 trust\n",
				"hostssl test sslonly 0::0/0 trust\n",
			])
		key_file = os.path.join(self.cluster.data_directory, 'server.key')
		crt_file = os.path.join(self.cluster.data_directory, 'server.crt')
		with open(key_file, 'w') as key:
			key.write(server_key)
		with open(crt_file, 'w') as crt:
			crt.write(server_crt)
		os.chmod(key_file, 0o700)
		os.chmod(crt_file, 0o700)

	def initialize_database(self):
		super().initialize_database()
		with self.cluster.connection(user = 'test') as db:
			db.execute(
				"""
CREATE USER nossl;
CREATE USER sslonly;
				"""
			)

	def test_ssl_mode_require(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'require'
		try:
			pg_driver.connect(
				user = 'nossl',
				database = 'test',
				host = host,
				port = port,
				**params
			)
			self.fail("successful connection to nossl user when sslmode = 'require'")
		except pg_exc.ClientCannotConnectError as err:
			for pq in err.database.failures:
				x = pq.error
				dossl = pq.ssl_negotiation
				if isinstance(x, pg_exc.AuthenticationSpecificationError) and dossl is True:
					break
			else:
				# let it show as a failure.
				raise
		with pg_driver.connect(
			host = host,
			port = port,
			user = 'sslonly',
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

	def test_ssl_mode_disable(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'disable'
		try:
			pg_driver.connect(
				user = 'sslonly',
				database = 'test',
				host = host,
				port = port,
				**params
			)
			self.fail("successful connection to sslonly user with sslmode = 'disable'")
		except pg_exc.ClientCannotConnectError as err:
			for pq in err.database.failures:
				x = pq.error
				if isinstance(x, pg_exc.AuthenticationSpecificationError) and not hasattr(pq, 'ssl_negotiation'):
					# looking for an authspec error...
					break
			else:
				# let it show as a failure.
				raise

		with pg_driver.connect(
			host = host,
			port = port,
			user = 'nossl',
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

	def test_ssl_mode_prefer(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'prefer'
		with pg_driver.connect(
			user = 'sslonly',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

		with pg_driver.connect(
			user = 'test',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.security, 'ssl')

		with pg_driver.connect(
			user = 'nossl',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

	def test_ssl_mode_allow(self):
		host, port = self.cluster.address()
		params = dict(self.params)
		params['sslmode'] = 'allow'

		# nossl user (hostnossl)
		with pg_driver.connect(
			user = 'nossl',
			database = 'test',
			host = host,
			port = port,
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, None)

		# test user (host)
		with pg_driver.connect(
			user = 'test',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.security, None)

		# sslonly user (hostssl)
		with pg_driver.connect(
			user = 'sslonly',
			host = host,
			port = port,
			database = 'test',
			**params
		) as c:
			self.assertEqual(c.prepare('select 1').first(), 1)
			self.assertEqual(c.security, 'ssl')

if __name__ == '__main__':
	unittest.main()
