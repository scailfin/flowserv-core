# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the database connector for PostgreSQL. Uses the psycopg
adapter to connect to the underlying database. For compatibility with the
SQLLite connection object a special Postgres connection object is implemented
that supports methods commit() and execute().
"""

import os

from psycopg2.extras import RealDictCursor

from flowserv.core.db.connector import DatabaseConnector


"""Driver-specific environment variables containing connection information for
the database.
"""
PG_FLOWSERV_HOST = 'PG_FLOWSERV_HOST'
PG_FLOWSERV_DATABASE = 'PG_FLOWSERV_DATABASE'
PG_FLOWSERV_USER = 'PG_FLOWSERV_USER'
PG_FLOWSERV_PASSWORD = 'PG_FLOWSERV_PASSWORD'
PG_FLOWSERV_PORT = 'PG_FLOWSERV_PORT'


class PostgresConnection(object):
    """Wrapper around a Psycopg2 connection object. This wrapper is implemented
    to achieve flexibility of flowServ with respect to the database system that
    is being used. The application is tested (and was originally designed)
    using SQLLite3. Unfortunately, there seem to be some differences in how
    query processing in flowServ was implements using SQLite3 and how the
    Psycopg2 module operates: in SQLite3 the execute() method is called
    directly on the database connection object and it returns a cursor. Also,
    by default the result rows in SQLLite3 are dictionaries. There is also a
    difference in how the two databases handle SQL query parameters.

    This wrapper object attempts to emulate the SQLite3 behavior in order to be
    able to use the same code to interact with the underlying database
    independently of the database system.

    We make a very strong assumption here. We assume that all SQL queries
    that are used by flowServ do not contain any '?' character other than the
    ones that are used to define query parameters. This assumption allows us to
    simply replace the '?' with '%s' to achieve compatibility between the
    different database systems that are currently supported.

    The wrapper implements the __enter__ and __exit__ methods for a context
    manager to enable usage of this class within with statements.
    """
    def __init__(self, con):
        """Initialize the database connection object.

        Parameters
        ----------
        con: DB-API 2.0 database connection
            Connection to underlying database
        """
        self.con = con
        self.cur = None

    def __enter__(self):
        """The enter method of the context manager simply returns the object
        itself.

        Returns
        -------
        flowserv.core.db.pg.PostgresConnection
        """
        return self

    def __exit__(self, type, value, traceback):
        """Ensure to close any open cursor and the database connection when
        the context manager exits. Returns False to ensure that any exception
        is re-raised.

        Returns
        -------
        bool
        """
        # Make sure to close any open cursors
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        # Close the database connections
        if self.con is not None:
            self.con.close()
            self.con = None
        return False

    def close(self):
        """Call the exit method when the database connection is closed."""
        self.__exit__(None, None, None)

    def commit(self):
        """Commit all changes to the database."""
        self.con.commit()

    def cursor(self):
        """Create a new cursor object. This method ensures that there is only
        one cursor open at the time.

        All cursors use the real dictionary cursor factory to for compatibility
        with result sets returned by the SQLLite3 database connection object.

        Returns
        -------
        psycopg2.cursor
        """
        if self.cur is not None:
            self.cur.close()
        self.cur = self.con.cursor(cursor_factory=RealDictCursor)
        return self.cur

    def execute(self, sql, args=None):
        """Execute a given SQL statement. If the list of argument values is
        given all parameters ('?') in the query are replaced with '%s'
        which is used by the Psycopg2 driver as query parameter. Here we make
        a strong assumption that every '?' character in the SQL query represents
        a query parameter.

        Returns the cursor after the query is being executed to allow fetchone
        and fetchall operations on the result.

        Parameters
        ----------
        sql: string
            SQL query
        args: tuple, optional
            Optional list of query parameters

        Returns
        -------
        psycopg2.cursor
        """
        self.cur = self.cursor()
        if args is not None:
            # Replace all query parameters. Note that code assumes that every
            # '?'' character in SQL query represents a parameter. It does not
            # account for cases where , for example, a '?' is part of a query
            # string.
            sql = sql.replace('?', '%s')
            self.cur.execute(sql, args)
        else:
            self.cur.execute(sql)
        return self.cur


class PostgresConnector(DatabaseConnector):
    """Database connector for PostgreSQL databases."""
    def __init__(self, connect_string=None):
        """Initialize connection information from the given connect string.
        Expects a connect string in the following format:

        {host}/{database name}:{user name}/{password}

        Note that the database and user name cannot contain the '/' character.
        Raises a ValueError if the connection string cannot be parsed.

        If the connection string is not given the necessary information to
        connect to the database is expected to be in the respective environment
        variables PG_FLOWSERV_HOST, PG_FLOWSERV_DATABASE, PG_FLOWSERV_USER, PG_FLOWSERV_PASSWORD.

        Parameters
        ----------
        connect_string: string, optional
            The connect string containing information about database host, name
            as well as user credentials.

        Raises
        ------
        ValueError
        """
        if connect_string is not None:
            # Get host name and port from the first part of the connect string
            # up until the first '/' character
            pos_1 = connect_string.index('/')
            self.host = connect_string[:pos_1]
            # The database name comes after the host name up until the first
            #  ':' character
            pos_2 = connect_string.index(':', pos_1 + 1)
            self.database = connect_string[pos_1+1:pos_2]
            # The user name is after the database name up until the next '/'
            # character followed by the password
            pos_3 = connect_string.index('/', pos_2 + 1)
            self.user = connect_string[pos_2+1:pos_3]
            self.password = connect_string[pos_3+1:]
            self.port = 5432
        else:
            self.host = os.environ.get(PG_FLOWSERV_HOST, 'localhost')
            self.port = int(os.environ.get(PG_FLOWSERV_PORT, '5432'))
            self.database = os.environ.get(PG_FLOWSERV_DATABASE, 'flowserv')
            self.user = os.environ.get(PG_FLOWSERV_USER, 'flowserv')
            self.password = os.environ.get(PG_FLOWSERV_PASSWORD, 'flowServ')

    @staticmethod
    def configuration():
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        connector = PostgresConnector()
        return [
            (PG_FLOWSERV_HOST, connector.host),
            (PG_FLOWSERV_PORT, str(connector.port)),
            (PG_FLOWSERV_DATABASE, connector.database),
            (PG_FLOWSERV_USER, connector.user),
            (PG_FLOWSERV_PASSWORD, connector.password),
        ]

    def connect(self):
        """Connect to the underlying PostgreSQL database.

        Returns
        -------
        flowserv.core.db.pg.PostgresConnection
        """
        import psycopg2
        return PostgresConnection(
            psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
        )

    def info(self, indent=''):
        """Get information about the underlying database. Outouts only the
        host and database name

        Parameters
        ----------
        indent: string, optional
             Optional indent when printing information string

        Returns
        -------
        string
        """
        return indent + 'postgres {} on {}'.format(self.database, self.host)

    def execute(self, schema_file):
        """Executing a given SQL script.

        Parameters
        ----------
        schema_file: string
            Path to the file containing the DML or DDL statements
        """
        with self.connect() as con:
            with open(schema_file) as f:
                # Note that the result of connect() is a connection object that
                # wrapps a database connection.
                con.cursor().execute(f.read())
            con.commit()
