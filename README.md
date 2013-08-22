sublime-sqlexec
===============

A Plugin for running SQL commands in Sublime Text.
Compatibility: Oracle, MySQL, PostgreSQL.

# Installation
Download the Zip file, extract it to your Sublime Text packages directory, and rename it to SQLExec
  
Some directories have to be defined in the PATH environment variable, according to the SGBD that you want to use: "mysql" executable for MySQL, "pgsql" executable for PostgreSQL, or "sqlplus" executable for Oracle

# Usage
Default shortcuts are :
ctrl+alt+e: swhitch connection
ctrl+e ctrl+q: execute query
ctrl+e ctrl+s: show tables records
ctrl+e ctrl+d: desc table