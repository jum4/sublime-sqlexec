import sublime, sublime_plugin, os, subprocess, re, shutil

def getResultFileName():
    return os.path.join(sublime.packages_path(), 'SQLExec', 'results')

def getTempFileName():
    return os.path.join(sublime.packages_path(), 'SQLExec', 'tmp')

def getOutputFileName():
    return os.path.join(sublime.packages_path(), 'SQLExec', 'output')

def getSelection(view):
    queries = []
    if view.sel():
        for region in view.sel():
            if region.empty():
                queries.append(view.substr(view.line(region)))
            else:
                queries.append(view.substr(region))

    return ''.join(queries).replace('\n', ' ')

def getFileCommand(view, filename):
    options        = view.settings().get('database')
    resultFilename = getResultFileName()
    tempFilename   = getTempFileName()

    if options['type'] == 'mysql':
        cmd = "mysql --table -h%s -P%d -u%s -p%s -D%s < \"%s\" > \"%s\"" # Execute all file
        cmd = cmd % ( options['host'], options['port'], options['user'], options['password'], options['database'], filename, resultFilename)
    elif options['type'] == 'pgsql':
        cmd =  "psql -h %s -p %d -U %s -d %s -f \"%s\" > \"%s\"" # Execute all file
        cmd = cmd % ( options['host'], options['port'], options['user'], options['database'], filename, resultFilename)
    elif options['type'] == 'oracle':
        tempFile = open(getTempFileName(), 'w')
        tempFile.write("SET LINESIZE 20000 TRIM ON TRIMSPOOL ON;\n")
        with open(filename) as f:
            for line in f.readlines():
                tempFile.write(line)
        tempFile.close()
        f.close()
        cmd = "sqlplus -S %s/%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SERVICE_NAME=%s))) < \"%s\" > \"%s\""
        cmd = cmd % ( options['user'], options['password'], options['host'], options['port'], options['database'], filename, resultFilename)
    return cmd

def getSqlCommand(view, sql):
    options        = view.settings().get('database')
    resultFilename = getResultFileName()
    
    if options['type'] == 'mysql':
        cmd = "mysql -f --table -h%s -P%d -u%s -p%s -D%s -e\"%s\" > \"%s\"" # Execute selected queries
        cmd = cmd % ( options['host'], options['port'], options['user'], options['password'], options['database'], sql, resultFilename)
    elif options['type'] == 'pgsql':
        cmd = "psql -h %s -p %d -U %s -d %s -c \"%s\" > \"%s\"" # Execute selected queries
        cmd = cmd % ( options['host'], options['port'], options['user'], options['database'], sql, resultFilename)
    elif options['type'] == 'oracle':
        tempFile = open(getTempFileName(), 'w')
        tempFile.write("SET LINESIZE 20000 TRIM ON TRIMSPOOL ON;\n")
        tempFile.write(sql)
        tempFile.close()    
        cmd = "sqlplus -S %s/%s@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d)))(CONNECT_DATA=(SERVICE_NAME=%s))) < \"%s\" > \"%s\""
        cmd = cmd % ( options['user'], options['password'], options['host'], options['port'], options['database'], getTempFileName(), resultFilename)
        print cmd
    return cmd

def fileIsEmpty(filename):
    if os.path.isfile(filename):
        statinfo = os.stat(filename)
        return statinfo.st_size == 0
    return True

def showResult(view):
    options = view.settings().get('database')

    if not fileIsEmpty(getResultFileName()):
        view=view.window().open_file(getResultFileName(), sublime.TRANSIENT)
        view.settings().set('word_wrap', False)
    else:
        if not fileIsEmpty(getOutputFileName()):
            with open(getTempFileName(), 'w') as temp:
                for line in open (getOutputFileName(), 'r'):
                    if line != "Warning: Using a password on the command line interface can be insecure.\n":
                        temp.write(line)
                temp.close()
                view=view.window().open_file(getTempFileName(), sublime.TRANSIENT)

def runCommand(window, cmd):
    print cmd
    with open(getOutputFileName(),"w") as out:
        subprocess.call(cmd, shell=True, stderr=out)
    out.close()
    showResult(window.active_view())
    clean()

def checkConfig(view):
    if not view.settings().has('database'):
        sublime.error_message('You have to configure database connection in your project\'s settings')
        return False
    else:
        options = view.settings().get('database')
        if not 'host' in options:
            sublime.error_message('You have to define an host for this connection')
            return False
        if not 'port' in options:
            sublime.error_message('You have to define the port for this connection')
            return False
        if not 'database' in options:
            sublime.error_message('You have to define the database name for this connection')
            return False

    return True

def clean():
    if os.path.isfile(getResultFileName()):
        os.remove(getResultFileName())
    if os.path.isfile(getTempFileName()):
        os.remove(getTempFileName())
    if os.path.isfile(getOutputFileName()):
        os.remove(getOutputFileName())

class SqlExec(sublime_plugin.WindowCommand):
    def run(self):
        sql = getSelection(self.window.active_view())
        if sql != '' and checkConfig(self.window.active_view()):
            cmd = getSqlCommand(self.window.active_view(), sql)
            runCommand(self.window, cmd)

class SqlExecFile(sublime_plugin.WindowCommand):
    def run(self):
        if checkConfig(self.window.active_view()):
            self.window.active_view().run_command('save')
            filename = self.window.active_view().file_name()
            cmd = getFileCommand(self.window.active_view(), filename)
            runCommand(self.window, cmd)