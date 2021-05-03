# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for registration and password reset in the user manager."""

import pytest
import time

from flowserv.model.auth import OpenAccessAuth
from flowserv.model.user import UserManager

import flowserv.error as err


def test_activate_user(database):
    """Test creating inactive users and activating them."""
    # -- Test creating an inactive user ---------------------------------------
    username = 'nouser@me.com'
    password = 'pwd1'
    with database.session() as session:
        users = UserManager(session)
        user = users.register_user(username, password, verify=True)
        user_id = user.user_id
        # Attempt to login will raise error
        with pytest.raises(err.UnknownUserError):
            users.login_user(username, password)
    # -- Test activate user ---------------------------------------------------
    with database.session() as session:
        # After activating the user login should succeed.
        users = UserManager(session)
        auth = OpenAccessAuth(session)
        active_user = users.activate_user(user_id)
        assert active_user.user_id == user_id
        assert active_user.name == username
        assert active_user.api_key is None
        user = users.login_user(username, password)
        assert user.name == username
        assert user.api_key is not None
        assert auth.authenticate(user.api_key.value).user_id == user_id
        # Activate the same user twice should not raise an error
        active_user = users.activate_user(user_id)
        assert active_user.user_id == user_id
        assert active_user.name == username
        assert active_user.api_key is not None


def test_list_users(database):
    """Test listing and searching for users."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with four active and one inactive user.
    with database.session() as session:
        users = UserManager(session)
        users.register_user('abc@me.com', 'pwd1', verify=False)
        users.register_user('ade@me.com', 'pwd1', verify=False)
        users.register_user('abc@you-and-me.com', 'pwd1', verify=False)
        users.register_user('def@me.com', 'pwd1', verify=False)
        u = users.register_user('xyz@me.com', 'pwd1', verify=True)
        inactive_user_id = u.user_id
    # -- Test user listing ----------------------------------------------------
    with database.session() as session:
        # There should be four users in the returned list.
        users = UserManager(session)
        assert len(users.list_users()) == 4
    with database.session() as session:
        # After activating the inactive user, the listing contains five users.
        users = UserManager(session)
        users.activate_user(inactive_user_id)
        assert len(users.list_users()) == 5
    # -- Test query prefixes --------------------------------------------------
    with database.session() as session:
        # Query users by name prefix.
        users = UserManager(session)
        assert len(users.list_users(prefix='a')) == 3
        assert len(users.list_users(prefix='ab')) == 2
        assert len(users.list_users(prefix='ade')) == 1


def test_register_user(database):
    """Test registering a new user."""
    # -- Test creating an active user -----------------------------------------
    with database.session() as session:
        users = UserManager(session)
        auth = OpenAccessAuth(session)
        reg_user = users.register_user('first.user@me.com', 'pwd1')
        assert reg_user.api_key is None
        user_1 = users.login_user('first.user@me.com', 'pwd1')
        assert user_1.name == 'first.user@me.com'
        user_id_1 = auth.authenticate(user_1.api_key.value).user_id
        assert user_1.user_id == user_id_1
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        users = UserManager(session)
        # Register user with existing email address raises error
        with pytest.raises(err.DuplicateUserError):
            users.register_user('first.user@me.com', 'pwd1')
        # Providing invalid email or password will raise error
        with pytest.raises(err.ConstraintViolationError):
            users.register_user(None, 'pwd1')
        with pytest.raises(err.ConstraintViolationError):
            users.register_user(' \t', 'pwd1')
        with pytest.raises(err.ConstraintViolationError):
            users.register_user('a' * 513, 'pwd1')
        with pytest.raises(err.ConstraintViolationError):
            users.register_user('valid.name@me.com', ' ')


def test_reset_password(database):
    """Test resetting a user password."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one active user.
    username = 'first.user@me.com'
    password = 'pwd1'
    with database.session() as session:
        # After a reset request has been send the previous password should
        # still be valid.
        users = UserManager(session)
        auth = OpenAccessAuth(session)
        user_id = users.register_user(username, password).user_id
    # -- Test reset password --------------------------------------------------
    with database.session() as session:
        users = UserManager(session)
        auth = OpenAccessAuth(session)
        # Ensure login works prior to reset request.
        token = users.login_user(username, password).api_key.value
        assert auth.authenticate(token).user_id == user_id
        request_id = users.request_password_reset(username)
        password = 'mypwd'
        user = users.reset_password(request_id=request_id, password=password)
        assert user.user_id == user_id
        # After resetting the password the previous API key for the user is
        # invalid
        with pytest.raises(err.UnauthenticatedAccessError):
            auth.authenticate(token)
        token = users.login_user('first.user@me.com', 'mypwd').api_key.value
        assert auth.authenticate(token).user_id == user_id
    # -- Test login after request ---------------------------------------------
    with database.session() as session:
        # After a reset request has been send the previous password should
        # still be valid.
        users = UserManager(session)
        auth = OpenAccessAuth(session)
        users.request_password_reset(username)
        token = users.login_user(username, password).api_key.value
        assert auth.authenticate(token).user_id == user_id
    # -- Test request reset for unknown user ----------------------------------
    with database.session() as session:
        users = UserManager(session)
        assert users.request_password_reset('unknown@me.com') is not None
    # --Error cases -----------------------------------------------------------
    with database.session() as session:
        # An error is raised when (i) trying to use a request for an unknown
        # user, (ii) a previously completed reset request, or (iii) an unknown
        # request identifier to reset a user password
        users = UserManager(session)
        with pytest.raises(err.UnknownRequestError):
            users.reset_password(request_id=request_id, password=password)
        with pytest.raises(err.UnknownRequestError):
            users.reset_password(request_id='UNKNOWN', password=password)
        with pytest.raises(err.UnknownRequestError):
            users.reset_password(request_id='unknown', password=password)


def test_reset_request_timeout(database):
    """Test resetting a user password using a request identifier that has
    timed out.
    """
    # -- Setup ----------------------------------------------------------------
    #
    # Create database with single active user.
    username = 'first.user@me.com'
    with database.session() as session:
        users = UserManager(session)
        users.register_user(username, 'pwd1')
    # -- Test password reset after timed-out ----------------------------------
    with database.session() as session:
        # Request a password reset and then sleep for a period of time that is
        # lonker than the token timeout period.
        users = UserManager(session, token_timeout=1)
        request_id = users.request_password_reset(username)
        time.sleep(2)
        with pytest.raises(err.UnknownRequestError):
            users.reset_password(request_id=request_id, password='mypwd')
