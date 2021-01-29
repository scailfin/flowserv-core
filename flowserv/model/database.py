# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Wrapper for database connections. The wrapper is used to open database
sessions as well as to create a fresh database.
"""

from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from typing import Optional

import os

from flowserv.model.base import Base

import flowserv.config as config
import flowserv.util as util


"""Database connection Url for test purposes."""
TEST_URL = 'sqlite:///:memory:'


def TEST_DB(dirname: str, filename: Optional[str] = 'test.db'):
    """Get connection Url for a databse file."""
    return 'sqlite:///{}'.format(os.path.join(dirname, filename))


SQLITE_DB = TEST_DB


class DB(object):
    """Wrapper to establish a database connection and create the database
    schema.
    """
    def __init__(
        self, connect_url: str, web_app: Optional[bool] = False,
        echo: Optional[bool] = False
    ):
        """Initialize the database object from the given configuration object.

        Parameters
        ----------
        connect_url: string
            SQLAlchemy database connect Url string.
        web_app: boolean, default=False
            Use scoped sessions for web applications if set to True.
        echo: bool, default=False
            Flag that controls whether the created engine is verbose or not.
        """
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

    def init(self) -> DB:
        """Create all tables in the database model schema. This will also
        register the default user. The password for the user is a random UUID
        since the default user is not expected to login (but be used only in
        open access policies).
        """
        # Add import for modules that contain ORM definitions.
        import flowserv.model.base  # noqa: F401
        # Drop all tables first before creating them
        Base.metadata.drop_all(self._engine)
        Base.metadata.create_all(self._engine)
        # Create the default user.
        with self.session() as session:
            from passlib.hash import pbkdf2_sha256
            from flowserv.model.base import User
            user = User(
                user_id=config.DEFAULT_USER,
                name=config.DEFAULT_USER,
                secret=pbkdf2_sha256.hash(util.get_unique_identifier()),
                active=True
            )
            session.add(user)
        return self

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

    def close(self):
        """Close the session."""
        self.__exit__(None, None, None)

    def open(self):
        """Get a reference to the database session object."""
        return self.__enter__()
