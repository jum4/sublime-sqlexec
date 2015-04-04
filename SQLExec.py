

import sublime, sublime_plugin, sys, os, socket

sys.path.append(os.path.join(os.path.dirname(__file__), "lib"))

import pymysql as api_mysql
import postgresql.driver.dbapi20 as api_pgsql
import pytds as api_sqlserver
import ora as api_oracle

from prettytable import from_db_cursor

connection = None
history = []

socket.setdefaulttimeout(10)

# This class manage persisting connections
# You don't have to call connect method, it's automatically called when it's necessary
class Connection:
    def __init__(self, options):
        self.settings = sublime.load_settings(options['type'] + ".sqlexec").get('sql_exec')
        self.api = eval("api_" + options['type'])
        self.options  = options
        self.db = None
    def connect(self):
        if None == self.db:
            if self.options['type'] == 'sqlserver':
                self.db = self.api.connect(port=self.options['port'], server=self.options['host'], database=self.options['database'], user=self.options['username'], password=self.options['password'])
            else:
                self.db = self.api.connect(port=self.options['port'], host=self.options['host'], database=self.options['database'], user=self.options['username'], password=self.options['password'])
            self.db.autocommit = True
    def desc(self):
        try:
            self.connect()
            query = self.settings['queries']['desc']
            cur = self.db.cursor()
            cur.execute(query);
            result = cur.fetchall()
            self.tables = tuple(x[0] for x in result)
            return self.tables
        except self.api.Error as e:
            displayPanel('Error: %s' % e)
    def query(self, query):
        try:
            self.connect()
            historize(query)
            cur = self.db.cursor()
            cur.execute(query)
            table = from_db_cursor(cur)
            return table.get_string()
        except self.api.Error as e:
            displayPanel('Error: %s' % e)
    def descTable(self, tableName):
        return self.query(self.settings['queries']['desc table'] % tableName)
    def showRecords(self, tableName):
        return self.query(self.settings['queries']['show records'] % tableName)

def displayPanel(text):
    panel = sublime.active_window().create_output_panel('SQLExec')
    sublime.active_window().run_command("show_panel", {"panel": "output.SQLExec"})
    panel.set_read_only(False)
    panel.set_syntax_file('Packages/SQL/SQL.tmLanguage')
    panel.run_command('append', {'characters': text})
    panel.set_read_only(True)

def displayWindow(text):
    panel = None
    for view in sublime.active_window().views():
        if view.name() == 'SQLExec':
            panel = view
            panel.set_read_only(False)
            sublime.active_window().focus_view(panel)
            view.run_command('select_all')
            view.run_command('cut')
    if panel == None:
        panel = sublime.active_window().new_file()
        panel.set_scratch(True)
        panel.set_name('SQLExec')
        panel.set_read_only(False)
        panel.set_syntax_file('Packages/SQL/SQL.tmLanguage')
    panel.run_command('append', {'characters': text})
    panel.set_read_only(True)

def display(text):
    if sublime.load_settings("SQLExec.sublime-settings").get('show_result_on_window'):
        displayWindow(text)
    else:
        displayPanel(text)

def historize(query):
    global history

    if (query != lastQuery()):
        history.insert(0, query)

def queries():
    global history

    return history

def clearHistory():
    global history

    history = []

def lastQuery():
    global history

    if 0 < len(history):
        return history[0]
    return ''

def connect(name):
    global connection
    options = sublime.load_settings("SQLExec.sublime-settings").get('connections')[name]
    connection = Connection(options)

def getConnection():
    global connection

    if connection != None:
        return connection
    else:
        displayPanel('No active connection')
        exit()

def getConnectionsList():
    names = []
    connections = sublime.load_settings("SQLExec.sublime-settings").get('connections')
    for connection in connections:
        names.append(connection)
    names.sort()
    return names

class sqlListConnection(sublime_plugin.WindowCommand):
    def run(self):
        def changeConnection(index):
            names = getConnectionsList()
            connect(names[index])
            sublime.status_message(' SQLExec: switched to %s' % names[index])
        sublime.active_window().show_quick_panel(getConnectionsList(), changeConnection)

class sqlDesc(sublime_plugin.WindowCommand):
    def run(self):
        def selectTable(index):
            if index > -1:
                display(getConnection().descTable(getConnection().tables[index]))
        tables = getConnection().desc()
        if len(tables) > 0:
            sublime.active_window().show_quick_panel(tables, selectTable)
        else:
            displayPanel('There is no table in this database')

class sqlShowRecords(sublime_plugin.WindowCommand):
    def run(self):
        def showTableRecords(index):
            if index > -1:
                display(getConnection().showRecords(getConnection().tables[index]))

        tables = getConnection().desc()
        if len(tables) > 0:
            sublime.active_window().show_quick_panel(tables, showTableRecords)
        else:
            displayPanel('There is no table in this database')

class sqlRedo(sublime_plugin.WindowCommand):
    def run(self):
        result = getConnection().query(lastQuery())
        if result != '':
            display(result)
        else:
            displayPanel('Query executed successful')

class sqlQuery(sublime_plugin.WindowCommand):
    def run(self):
        def executeQuery(query):
            result = getConnection().query(query)
            if result != '':
                display(result)
            else:
                displayPanel('Query executed successful')

        sublime.active_window().show_input_panel('Enter query', lastQuery(), executeQuery, None, None)

class sqlHistory(sublime_plugin.WindowCommand):
    def run(self):
        def executeHistoryQuery(index):
            if index > -1:
                query = queries()[index]
                display(query, getConnection().query(query))

        sublime.active_window().show_quick_panel(queries(), executeHistoryQuery)

class sqlClearHistory(sublime_plugin.WindowCommand):
    def run(self):
        clearHistory()
