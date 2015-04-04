# Wrapper for Oracle instant client executable. Because Oracle has not pure python client.
# We simulate python DB-API 2.0, and implement only objects and methods used by SQLExec

import subprocess, tempfile, os

def connect(*args, **kwargs):
    return Db(*args, **kwargs)

class Error(BaseException):
    pass

class Db(object):
    def __init__(self, host="localhost", user=None, password="", database=None, port=None, autocommit=True, db=None, passwd=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.autocommit_mode = autocommit
    def cursor(self):
        return Cursor(self)

class Cursor(object):
    def __init__(self, db):
        self.db = db
    def execute(self, query):
        command = 'sqlplus -s %s/%s@%s:%s/%s' % (self.db.user, self.db.password, self.db.host, self.db.port, self.db.database)
        tmp = tempfile.NamedTemporaryFile(mode = 'w', delete = False, suffix='.sql')
        tmp.write('set colsep ";";'+ "\n")
        tmp.write('set pagesize 50000;'+ "\n")
        tmp.write('set linesize 10000;'+ "\n")
        tmp.write('set tab off;'+ "\n")
        tmp.write('set trimspool on;'+ "\n")
        tmp.write('set term off;'+ "\n")
        tmp.write("set sqlprompt  ''; \n")
        tmp.write('set UNDERLINE off;'+ "\n")
        tmp.write('set feedback off;'+ "\n")
        tmp.write('set TERMOUT off;'+ "\n")
        tmp.write(query)
        tmp.close()
        cmd = '%s < "%s"' % (command, tmp.name)
        print('cmd = ' + cmd)
        result, errors = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True).communicate()
        if not result and errors:
            raise Error(errors.decode('utf-8', 'replace').replace('\r', ''))

        self.results = []
        for row in result.decode('utf-8', 'replace').replace('\r', '').splitlines():
            print('row = ' + row)
            result = []
            for column in row.split(';'):
                result.append(column.strip())
            self.results.append(result)

        self.results.pop(0)

        self.description = []
        for column in self.results[0]:
            self.description.append([column.strip()])

        self.results.pop(0)
        self.rowcount = len(self.results)
        # os.unlink(tmp.name)
    def fetchall(self):
        return self.results
    def description(self):
        return True
