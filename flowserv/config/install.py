# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Classes used when installing the package and setting up the database."""

import pkg_resources

from flowserv.core.db.driver import DatabaseDriver


class DB(object):
    """Setup the benchmark database from DML and DDL scripts that are included
    with the package.
    """
    @staticmethod
    def init(dbms_id=None, connect_string=None):
        """Initialize the benchmark database from DML and DDL scripts that are
        included with the package.

        The given parameters are used to establish the connection to the
        database.

        Parameters
        ----------
        dbms_id: string
            Unique identifier for the database management system
        connect_string: string
            Database system specific information to establish a connection to
            an existing database
        """
        # Add names of files here if they contain statements to be executed
        # when the database is initialized. Files are executed in the order
        # of the list.
        scripts = [
            'config/resources/db/repository.sql'
        ]
        # Assumes that all script files are distributed as part of the current
        # package
        pkg_name = __package__.split('.')[0]
        # Create list of filenames from the relative file paths in the scripts
        # listing
        files = list()
        for script_file in scripts:
            filename = pkg_resources.resource_filename(pkg_name, script_file)
            files.append(filename)
        # Execute the script files
        DatabaseDriver.execute(
            scripts=files,
            dbms_id=dbms_id,
            connect_string=connect_string
        )
