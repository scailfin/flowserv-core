# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""The database connector defines the interface that is implemented for each
database system that is supported by the database driver. The connector captures
details of the underlying database system in a system-specific connection string
that contains all the information necessary to establish a database connection.
"""

from abc import abstractmethod


class DatabaseConnector(object):
    """The database connector defines the interface to open a connection to the
    database system that is used by the application. There should be different
    implementations of this class for different database systems.

    The connector also implements the method that is used to initialize database
    tables from a script of SQL statements.
    """
    @abstractmethod
    def connect(self):
        """Connect to the underlying database.

        Returns
        -------
        DB-API 2.0 database connection
        """
        raise NotImplementedError()

    @abstractmethod
    def info(self, indent=''):
        """Get information about the underlying database.

        Parameters
        ----------
        indent: string, optional
             Optional indent when printing information string

        Returns
        -------
        string
        """
        raise NotImplementedError()

    @abstractmethod
    def execute(self, schema_file):
        """Executing a given SQL script.

        Parameters
        ----------
        schema_file: string
            Path to the file containing the DML or DDL statements
        """
        raise NotImplementedError()
