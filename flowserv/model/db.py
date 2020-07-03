# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Wrapper for database connections. The wrapper is used to open database
sessions as well as to create a fresh database.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from flowserv.model.base import Base

import flowserv.config.db as config


"""Database connection Url for test purposes."""
TEST_URL = 'sqlite:///:memory:'


class DB(object):
    """Wrapper to establish a database connection and create the database
    schema.
    """
    def __init__(self, connect_url=None, web_app=False, echo=False):
        """Initialize the database connection string. If no connect string is
        given an attempt is made to access the value in the respective
        environment variable. If the variable is not set an error is raised.

        Parameters
        ----------
        connect_url: string, optional
            Database connection string.
        web_app: bool, optional
            Use scoped sessions for web applications if set to True.
        echo: bool, optional
            Flag that controlls whether the created engine is verbose or not.

        Raises
        ------
        flowserv.error.MissingConfigurationError
        """
        if connect_url is None:
            connect_url = config.DB_CONNECT(raise_error=True)
        if echo:
            import logging
            logging.info('Connect to database Url %s' % (connect_url))
        self.engine = create_engine(connect_url, echo=echo)
        if web_app:
            self.session = scoped_session(sessionmaker(bind=self.engine))
        else:
            self.session = sessionmaker(bind=self.engine)()

    def init(self):
        """Create all tables in the database model schema."""
        # Add import for modules that contain ORM definitions.
        import flowserv.model.base  # noqa: F401
        # Drop all tables first before creating them
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
