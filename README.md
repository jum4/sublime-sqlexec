sublime-sqlexec
===============

A Plugin for running SQL commands in Sublime Text.

Compatibility: MySQL, PostgreSQL.

Oracle is coming soon.

# Installation

### With Package Control ###

Look for the package named `PhpNamespace`.

### With Git ###

Download the Zip file, extract it to your Sublime Text packages directory, and rename it to SQLExec

Some directories have to be defined in the PATH environment variable, according to the SGBD that you want to use: "mysql" executable for MySQL, "pgsql" executable for PostgreSQL, or "sqlplus" executable for Oracle ( Not tested )

You can also specify full path for these command in settings :

( Preferences > Package Settings > SQLExec > Settings - User )

```json
    "sql_exec.commands": {
        "mysql" : "/usr/bin/mysql"
    },

```

# Sample configuration file
```json
{
    "connections": {
        "Connection 1": {
            "type"    : "mysql",
            "host"    : "127.0.0.1",
            "port"    : 3306,
            "username": "user",
            "password": "password",
            "database": "dbname"
        },
        "Connection 2": {
            "type"    : "pgsql",
            "host"    : "psql.server.fr",
            "port"    :  5432,
            "username": "anotheruser",
            "password": "password",
            "database": "dbname"
        }
    }
}
```
# Usage
Default shortcuts are :
- `ctrl+alt+e` : swhitch connection
- `ctrl+e` `ctrl+e` : execute selected query
- `ctrl+e` `ctrl+q` : type a query
- `ctrl+e` `ctrl+s` : show tables records
- `ctrl+e` `ctrl+d` : desc table
