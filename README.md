sublime-sqlexec
===============

A Plugin for running SQL commands in Sublime Text. 
Compatibility: Oracle, MySQL, PostgreSQL.

# Installation
  TODO
  
Some directories have to be defined in the PATH environment variable, according to the sgbd you want to use: "mysql" executable for MySQL, "pgsql" executable for PostgreSQL, or "sqlplus" executable for Oracle

# Configuration
In your project settings, add :
        "settings":
        {
            "database":
            {
                "type"       : "TYPE",
                "host"       : "DATABASE_HOST_OR_IP",
                "port"       : PORT NUMBER,
                "database"   : "DATABASE_NAME",
                "user"       : "YOUR_USERNAME",
                "password"   : "YOUR_PASSWORD"
            }
        },
    
Type can be one of : "oracle", "pgsql", "mysql". For PostgreSQL your need an pgpass.conf file for authenticate.
    
# Usage
Default shortcuts are :
  CTRL+E : Execute selected query or current line
  CTRL+SHIFT+E : Execute whole file
