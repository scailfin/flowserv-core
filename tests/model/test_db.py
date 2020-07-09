# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the database manager."""

import os
import pytest

from sqlalchemy.exc import IntegrityError

from flowserv.model.base import User
from flowserv.model.db import DB, TEST_URL

import flowserv.config.db as config
import flowserv.error as err


@pytest.mark.parametrize(
    'web_app,echo',
    [(True, True), (True, False), (False, True), (False, False)]
)
def test_db_webapp(web_app, echo):
    """Basic tests to ensure that the different parameter options for DB
    instantiation work without generating errors.
    """
    db = DB(connect_url=TEST_URL, web_app=web_app, echo=echo)
    db.init()


def test_missing_connect_error():
    """Error if connect URL is not given and environment variable is not
    set.
    """
    if config.FLOWSERV_DB in os.environ:
        del os.environ[config.FLOWSERV_DB]
    with pytest.raises(err.MissingConfigurationError):
        DB()


def test_session_scope():
    """Test the session scope object."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    with db.session() as session:
        session.add(User(user_id='U', name='U', secret='U', active=True))
    # Query all users. Expects one object in the resulting list.
    with db.session() as session:
        assert len(session.query(User).all()) == 1
    # Error when adding user with duplicate key.
    with pytest.raises(IntegrityError):
        with db.session() as session:
            session.add(User(user_id='U', name='U', secret='U', active=True))
    # Query all users. Still expects one object in the resulting list.
    with db.session() as session:
        assert len(session.query(User).all()) == 1
    # Raise an exception within the session scope.
    with pytest.raises(ValueError):
        with db.session() as session:
            session.add(User(user_id='W', name='W', secret='W', active=True))
            raise ValueError('some error')
    # Query all users. Still expects one object in the resulting list.
    with db.session() as session:
        assert len(session.query(User).all()) == 1
