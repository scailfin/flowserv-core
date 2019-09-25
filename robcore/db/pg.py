# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the database connector for PostgreSQL. Uses the psycopg
adapter to connect to the underlying database.
"""

from robcore.db.connector import DatabaseConnector


class PostgresConnector(DatabaseConnector):
    """Database connector for PostgreSQL databases."""
    def __init__(self, connect_string):
        """Initialize connection information from the given connect string.
        Expects a connect string in the following format:

        {host}/{database name}:{user name}/{password}

        Note that the database and user name cannot contain the '/' character.

        Raises a ValueError if the connection string cannot be parsed.

        Parameters
        ----------
        connect_string: string
            The connect string containing information about database host, name
            as well as user credentials.

        Raises
        ------
        ValueError
        """
        # Get host name and port from the first part of the connect string up
        # until the first '/' character
        pos_1 = connect_string.index('/')
        self.host = connect_string[:pos_1]
        # The database name comes after the host name up until the first ':'
        # character
        pos_2 = connect_string.index(':', pos_1 + 1)
        self.database = connect_string[pos_1+1:pos_2]
        # The user name is after the database name up until the next '/'
        # character followed by the password
        pos_3 = connect_string.index('/', pos_2 + 1)
        self.user = connect_string[pos_2+1:pos_3]
        self.password = connect_string[pos_3+1:]

    def connect(self):
        """Connect to the underlying PostgreSQL database.

        Returns
        -------
        DB-API 2.0 database connection
        """
        import psycopg2
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password
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
                con.cursor().execute(f.read())
            con.commit()
