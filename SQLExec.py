import sublime, sublime_plugin, tempfile, os, subprocess

connection = None

class Connection:
    def __init__(self, options):
        self.settings = sublime.load_settings(options.type + ".sqlexec").get('sql_exec')
        self.options  = options

    def _buildCommand(self, options):
        return self.settings['command'] + ' ' + ' '.join(options) + ' ' + self.settings['args'].format(options=self.options)

    def _getCommand(self, options, queries):
        command  = self._buildCommand(options)

        self.tmp = tempfile.NamedTemporaryFile(mode = 'w', delete = False)
        for query in queries:
            self.tmp.write(query)
        self.tmp.close()

        cmd = '%s < "%s"' % (command, self.tmp.name)

        return Command(cmd)

    def execute(self, queries):
        command = self._getCommand(self.settings['options'], queries)
        command.show()
        os.unlink(self.tmp.name)

    def desc(self):
        query = self.settings['queries']['desc']['query']
        command = self._getCommand(self.settings['queries']['desc']['options'], query)

        tables = []
        results = command.run()
        for result in results:
            try:
                tables.append(result.split('|')[1].strip())
            except IndexError:
                pass

        os.unlink(self.tmp.name)

        return tables

    def descTable(self, tableName):
        query = self.settings['queries']['desc table']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'], query)
        command.show()

        os.unlink(self.tmp.name)

    def showTableRecords(self, tableName):
        query = self.settings['queries']['show records']['query'] % tableName
        command = self._getCommand(self.settings['queries']['desc table']['options'], query)
        command.show()

        os.unlink(self.tmp.name)

class Command:
    def __init__(self, text):
        self.text = text

    def _run(self):
        result = tempfile.NamedTemporaryFile(mode = 'w', delete = False)
        output = tempfile.NamedTemporaryFile(mode = 'r+', delete = False)
        command = '%s > "%s"' % (self.text, result.name)
        result.close()
        subprocess.call(command, shell=True, stderr=output)

        if os.path.getsize(output.name) > 0:
            output.seek(0)
            print(output.read())
            sublime.active_window().run_command("show_panel", {"panel": "console"})
        output.close()
        os.unlink(output.name)

        return open(result.name, 'r')
        
    def run(self):
        results = []
        result = self._run()
        for line in result:
            results.append(line)
        result.close()
        os.unlink(result.name)

        return results

    def show(self):
        result = self._run()
        if os.path.getsize(result.name) > 0:
            sublime.active_window().open_file(result.name, sublime.TRANSIENT)
            sublime.active_window().active_view().settings().set('word_wrap', False)
        result.close()
        os.unlink(result.name)

class Selection:
    def __init__(self, view):
        self.view = view
    def getQueries(self):
        text = []
        if self.view.sel():
            for region in self.view.sel():
                if region.empty():
                    text.append(self.view.substr(self.view.line(region)))
                else:
                    text.append(self.view.substr(region))
        return text

class Options:
    def __init__(self, name):
        self.name     = name
        connections = sublime.load_settings("SQLExec.sublime-settings").get('connections')
        self.type     = connections[self.name]['type']
        self.host     = connections[self.name]['host']
        self.port     = connections[self.name]['port']
        self.username = connections[self.name]['username']
        self.password = connections[self.name]['password']
        self.database = connections[self.name]['database']

    def __str__(self):
        return self.name

    @staticmethod
    def list():
        names = []
        connections = sublime.load_settings("SQLExec.sublime-settings").get('connections')
        for connection in connections:
            names.append(connection)
        return names

def sqlChangeConnection(index):
    global connection
    names = Options.list()
    options = Options(names[index])
    connection = Connection(options)
    sublime.status_message(' SQLExec: switched to %s' % names[index])

def showTableRecords(index):
    global connection
    if index > -1:
        if connection != None:
            tables = connection.desc()
            connection.showTableRecords(tables[index])
        else:
            sublime.error_message('No active connection')

def descTable(index):
    global connection
    if index > -1:
        if connection != None:
            tables = connection.desc()
            connection.descTable(tables[index])
        else:
            sublime.error_message('No active connection')

class sqlDesc(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection != None:
            tables = connection.desc()
            sublime.active_window().show_quick_panel(tables, descTable)
        else:
            sublime.error_message('No active connection')

class sqlList(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection != None:
            tables = connection.desc()
            sublime.active_window().show_quick_panel(tables, showTableRecords)
        else:
            sublime.error_message('No active connection')

class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        global connection
        if connection != None:
            selection = Selection(self.window.active_view())
            connection.execute(selection.getQueries())
        else:
            sublime.error_message('No active connection')

class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        sublime.active_window().show_quick_panel(Options.list(), sqlChangeConnection)
