# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for login and logout functionality."""

import pytest

from flowserv.model.auth import DefaultAuthPolicy, OpenAccessAuth
from flowserv.model.user import UserManager

import flowserv.error as err
import flowserv.tests.model as model


@pytest.mark.parametrize('authcls', [DefaultAuthPolicy, OpenAccessAuth])
def test_authenticate_user(database, authcls):
    """Test user login and logout. Uses a database with two active and one
    inactive user to validate that active users can login and logout while
    inactive users cannot login.
    """
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two active and one inactive users.
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        user_2 = model.create_user(session, active=True)
        user_3 = model.create_user(session, active=False)
    # -- Test login -----------------------------------------------------------
    with database.session() as session:
        users = UserManager(session)
        token_1 = users.login_user(user_1, user_1).api_key.value
        token_2 = users.login_user(user_2, user_2).api_key.value
    # -- Test authentication --------------------------------------------------
    with database.session() as session:
        auth = authcls(session)
        # Authentication of logged-in users using the API key should return the
        # respective user identifier.
        assert auth.authenticate(token_1).user_id == user_1
        assert auth.authenticate(token_2).user_id == user_2
    # -- Test logout ----------------------------------------------------------
    with database.session() as session:
        # Logout user 1. User 2 should still be able to authenticate while a
        # user that is logged out cannot.
        users = UserManager(session)
        auth = authcls(session)
        assert users.logout_user(token_1).user_id == user_1
        # Authenticating user 1 will raise exception.
        with pytest.raises(err.UnauthenticatedAccessError):
            auth.authenticate(token_1)
        # Logging out a user that is not logged in will not raise error>
        assert users.logout_user(token_1) is None
        # User 2 can still authenticate.
        assert auth.authenticate(token_2).user_id == user_2
    # -- Test re-login --------------------------------------------------------
    with database.session() as session:
        # Login user 1 again.
        users = UserManager(session)
        auth = authcls(session)
        token_1 = users.login_user(user_1, user_1).api_key.value
        # User 1 and 2 can now be authenticated again.
        assert auth.authenticate(token_1).user_id == user_1
        assert auth.authenticate(token_2).user_id == user_2
        # If a logged in user logs in the previous key does not become invalid.
        token_3 = users.login_user(user_1, user_1).api_key.value
        assert auth.authenticate(token_1).user_id == user_1
        assert auth.authenticate(token_3).user_id == user_1
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        users = UserManager(session)
        auth = authcls(session)
        # Attempt to authenticate unknown user raises error
        with pytest.raises(err.UnknownUserError):
            users.login_user('unknown', user_1)
        # Attempt to authenticate with invalid password will raises error
        with pytest.raises(err.UnknownUserError):
            users.login_user(user_1, user_2)
        # Inactive user 3 should not be able to login.
        with pytest.raises(err.UnknownUserError):
            users.login_user(user_3, user_3)
        # An error is raised when using an invalid API key.
        with pytest.raises(err.UnauthenticatedAccessError):
            assert auth.authenticate('UNKNOWN')
