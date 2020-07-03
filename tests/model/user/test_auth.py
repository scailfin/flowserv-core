# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for login and logout functionality."""

import pytest
import time

from passlib.hash import pbkdf2_sha256

from flowserv.model.auth import OpenAccessAuth
from flowserv.model.base import User
from flowserv.model.user import UserManager

import flowserv.error as err
import flowserv.util as util


"""Unique identifier for test users."""
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()
USER_3 = util.get_unique_identifier()


def init_db(db):
    """Create new database with three users. The first two users are active
    while the third user is not active. Returns an instance of the user manager
    and the authentication manager.
    """
    for user_id, active in [(USER_1, True), (USER_2, True), (USER_3, False)]:
        user = User(
            user_id=user_id,
            name=user_id,
            secret=pbkdf2_sha256.hash(user_id),
            active=active
        )
        db.session.add(user)
    db.session.commit()
    return UserManager(db), OpenAccessAuth(db)


def test_authenticate_user(database):
    """Test user login and logout."""
    users, auth = init_db(database)
    # Login user 1 and 2
    user_1 = users.login_user(USER_1, USER_1)
    user_2 = users.login_user(USER_2, USER_2)
    # Authenticate user 1
    assert auth.authenticate(user_1.api_key.value).user_id == USER_1
    # Authenticate user 2
    assert auth.authenticate(user_2.api_key.value).user_id == USER_2
    # Logout user 1
    api_key_1 = user_1.api_key.value
    loggedout_user = users.logout_user(user_1)
    assert loggedout_user.user_id == user_1.user_id
    assert loggedout_user.api_key is None
    # Authenticating user 1 will raise exception
    with pytest.raises(err.UnauthenticatedAccessError):
        auth.authenticate(api_key_1)
    # Logging out a user that is not logged in will not raise error
    users.logout_user(user_1)
    # User 2 can still authenticate
    assert auth.authenticate(user_2.api_key.value).user_id == USER_2
    # Re-login user 1 and authenticate
    user_1 = users.login_user(USER_1, USER_1)
    assert auth.authenticate(user_1.api_key.value).user_id == USER_1
    # If a user logs in again the previous key does not become invalid
    user_3 = users.login_user(USER_1, USER_1)
    assert auth.authenticate(user_1.api_key.value).user_id == USER_1
    assert auth.authenticate(user_3.api_key.value).user_id == USER_1
    # Attempt to authenticate unknown user raises error
    with pytest.raises(err.UnknownUserError):
        users.login_user('unknown', USER_1)
    # Attempt to authenticate with invalid password will raises error
    with pytest.raises(err.UnknownUserError):
        users.login_user(USER_1, USER_2)
    # Attempting to login for inactive user raises error
    with pytest.raises(err.UnknownUserError):
        users.login_user(USER_3, USER_3)


def test_login_timeout(database):
    """Test login after key expired."""
    # Set login timeout to one second
    users, auth = init_db(database)
    # Manipulate login timeout
    users.login_timeout = 1
    api_key = users.login_user(USER_1, USER_1).api_key.value
    # Wait for two seconds
    time.sleep(2)
    # Authenticating after timeout will raise error
    with pytest.raises(err.UnauthenticatedAccessError):
        auth.authenticate(api_key)
