=============
Configuration
=============


All components of the *Reproducible Open Benchmarks for Data Analysis Platform (ROB)* are configured using environment variables in an attempt to follow `The Twelve-Factor App methodology <https://12factor.net/>`_ for application development.


--------
Database
--------

Database connections are established using the environment variables **ROB_DBMS** and **ROB_DBCONNECT**.

- **ROB_DBMS**: Determines the type of the database system that is used. **ROB** currently supports the following two database systems: `SQLite <https://sqlite.org/index.html>`_ (identified by either ``SQLITE`` or ``SQLITE3``) and `PostgreSQL <https://www.postgresql.org/>`_ (identified by ``POSTGRES``, ``POSTGRESQL``, ``PSQL``, or ``PG``).

- **ROB_DBCONNECT**: Database connection information. The value is used to establish a connection with the database server. The format and content of the string is dependent on the database system that is being used.


Connect to SQLite
-----------------

The connection string (**ROB_DBCONNECT**) for a SQLLite database is simple the path to the database file.


Connect to PostgreSQL
---------------------

When connecting to a PostgreSQL database server the connection string in **ROB_DBCONNECT** should follow the following format:

.. line-block::

    {host}/{database}:{user}/{password}

Note that the database name and the user name cannot contain the character ``/``!
