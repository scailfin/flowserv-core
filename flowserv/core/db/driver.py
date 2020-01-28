# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The database driver is a static class that is used to get an instance of a
connector object that provides connectivity to the database management system
that is being used by the application for data management.

The intent of the driver is to hide the specifics of connecting to different
database systems from the application. Instead, the database system and the
underlying database are specified using a unique database system identifier and
a system-specific connection string.

The database driver is configured using the environment variable FLOWSERV_DBMS
as well as additional database system specific variables.
"""

import flowserv.config.db as config


"""Default database system identifier."""
SQLITE = ['SQLITE', 'SQLITE3']
POSTGRES = ['POSTGRES', 'POSTGRESQL', 'PSQL', 'PG']


class DatabaseDriver(object):
    """The database driver instantiates objects that provide connectivity to
    the database that is used by the application. The driver provides access
    to different database management systems.
    """
    @staticmethod
    def configuration(dbms_id=None):
        """Get a list of tuples with the names of additional configuration
        variables and their current values for the database connector that
        is specified either by the variable dbms_id or the environment variable
        FLOWSERV_DBMS.

        A ValueError is raised if an unknown database system identifier is
        given. The database system may raise additional errors if the connect

        Returns
        -------
        list((string, string))

        Raises
        ------
        ValueError
        """
        # If missing, get the database system identifier from the value of the
        # respective environment variable. Raises a ValueError if the result is
        # None
        if dbms_id is None:
            dbms_id = config.DB_IDENTIFIER(raise_error=False, default_value='')
        # Return the connector for the identified database management system.
        # Raises ValueError if the given identifier is unknown.
        if dbms_id.upper() in SQLITE:
            # -- SQLite database ----------------------------------------------
            from flowserv.core.db.sqlite import SQLiteConnector
            values = SQLiteConnector.configuration()
        elif dbms_id.upper() in POSTGRES:
            # -- PostgreSQL database ------------------------------------------
            from flowserv.core.db.pg import PostgresConnector
            values = PostgresConnector.configuration()
        else:
            values = list()
        return [(config.FLOWSERV_DB_ID, dbms_id)] + values

    @staticmethod
    def get_connector(dbms_id=None, connect_string=None):
        """Get a connector object for the database management system that is
        being used by the application. The system and database are specified
        using the optional argument values. Missing argument values are filled
        in from the respective environment variables.

        The dbms-identifier is used to identify the database management system
        that is being used by the application. The driver currently supports two
        different systems with the following identifiers (as synonyms):

        SQLite3: SQLITE or SQLITE3
        PostgreSQL: POSTGRES, POSTGRESQL, PSQL, or PG

        The connect string is a database system specific string containing
        information that is used by the respective system's connect method to
        establish the connection.

        A ValueError is raised if an unknown database system identifier is
        given. The database system may raise additional errors if the connect
        string is invalid.

        Parameters
        ----------
        dbms_id: string
            Unique identifier for the database management system
        connect_string: string
            Database system specific information to establish a connection to
            an existing database

        Returns
        -------
        flowserv.core.db.connector.DatabaseConnector

        Raises
        ------
        ValueError
        """
        # If missing, set the database system identifier using the value in the
        # respective environment variable. Raises a ValueError if the result is
        # None
        if dbms_id is None:
            dbms_id = config.DB_IDENTIFIER(raise_error=True)
        # Return the connector for the identified database management system.
        # Raises ValueError if the given identifier is unknown.
        if dbms_id.upper() in SQLITE:
            # -- SQLite database ----------------------------------------------
            from flowserv.core.db.sqlite import SQLiteConnector
            return SQLiteConnector(connect_string=connect_string)
        elif dbms_id.upper() in POSTGRES:
            # -- PostgreSQL database ------------------------------------------
            from flowserv.core.db.pg import PostgresConnector
            return PostgresConnector(connect_string=connect_string)
        else:
            raise ValueError("unknown database system '{}'".format(dbms_id))

    @staticmethod
    def execute(scripts, dbms_id=None, connect_string=None):
        """Execute a given list of SQL scripts. The scripts can for example
        contain DML and DDL statements that are used to intialize a database.

        The given parameters are used to establish the connection to the
        database.

        Parameters
        ----------
        scripts: list(string)
            List of file names for DML or DDL scripts
        dbms_id: string
            Unique identifier for the database management system
        connect_string: string
            Database system specific information to establish a connection to
            an existing database
        """
        # Get a database connector
        db = DatabaseDriver.get_connector(
            dbms_id=dbms_id,
            connect_string=connect_string
        )
        # Execute the database scripts
        for script_file in scripts:
            db.execute(script_file)
