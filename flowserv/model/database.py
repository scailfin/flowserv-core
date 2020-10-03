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

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from flowserv.model.base import Base

import flowserv.config.database as config


"""Database connection Url for test purposes."""
TEST_URL = 'sqlite:///:memory:'


class DB(object):
    """Wrapper to establish a database connection and create the database
    schema.
    """
    def __init__(self, connect_url=None, web_app=None, echo=False):
        """Initialize the database connection string. If no connect string is
        given an attempt is made to access the value in the respective
        environment variable. If the variable is not set an error is raised.

        Parameters
        ----------
        connect_url: string, default=None
            Database connection string.
        web_app: bool, default=None
            Use scoped sessions for web applications if set to True.
        echo: bool, default=False
            Flag that controlls whether the created engine is verbose or not.

        Raises
        ------
        flowserv.error.MissingConfigurationError
        """
        # Ensure that the connection URL is set.
        connect_url = config.DB_CONNECT(value=connect_url)
        # Get web_app flag from configuration.
        web_app = config.WEBAPP(value=web_app)
        # If the URL references a SQLite database ensure that the directory for
        # the database file exists (Issue #68).
        if connect_url.startswith('sqlite://'):
            dbdir = os.path.dirname(connect_url[9:])
            if dbdir:
                os.makedirs(dbdir, exist_ok=True)
        if echo:
            import logging
            logging.info('Connect to database Url %s' % (connect_url))
        self._engine = create_engine(connect_url, echo=echo)
        if web_app:
            self._session = scoped_session(sessionmaker(bind=self._engine))
        else:
            self._session = sessionmaker(bind=self._engine)

    def init(self):
        """Create all tables in the database model schema."""
        # Add import for modules that contain ORM definitions.
        import flowserv.model.base  # noqa: F401
        # Drop all tables first before creating them
        Base.metadata.drop_all(self._engine)
        Base.metadata.create_all(self._engine)

    def session(self):
        """Create a new database session instance. The sessoin is wrapped by a
        context manager to properly manage the session scope.

        Returns
        -------
        flowserv.model.database.SessionScope
        """
        return SessionScope(self._session())


class SessionScope(object):
    """Context manager for providing transactional scope around a series of
    database operations.
    """
    def __init__(self, session):
        """Initialize the database session.

        Parameters
        ----------
        session: sqlalchemy.orm.session.Session
            Database session.
        """
        self.session = session

    def __enter__(self):
        """Return the managed database session object.

        Returns
        -------
        sqlalchemy.orm.session.Session
        """
        return self.session

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Commit or rollback transaction depending on the exception type.
        Does not surpress any exceptions.
        """
        if exc_type is None:
            try:
                self.session.commit()
            except Exception:
                self.session.rollback()
                raise
            finally:
                self.session.close()
        else:
            try:
                self.session.rollback()
            finally:
                self.session.close()
