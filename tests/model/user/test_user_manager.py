# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for registration and password reset in the user manager."""

import pytest
import time

from flowserv.model.user.auth import OpenAccessAuth
from flowserv.model.user.base import UserManager

import flowserv.core.error as err
import flowserv.tests.db as db


def init_db(basedir):
    """Initialize the database. Returns an instance of the user manager
    and the authentication manager.
    """
    con = db.init_db(str(basedir)).connect()
    return UserManager(con), OpenAccessAuth(con)


def test_activate_user(tmpdir):
    """Test creating inactive users and activating them."""
    # Create an inactive user.
    users, auth = init_db(str(tmpdir))
    user = users.register_user('nouser@me.com', 'pwd1', verify=True)
    # Attempt to login will raise error
    with pytest.raises(err.UnknownUserError):
        users.login_user('nouser@me.com', 'pwd1')
    # After activating the user login should succeed
    active_user = users.activate_user(user.identifier)
    assert active_user.identifier == user.identifier
    assert active_user.name == user.name
    assert active_user.api_key is None
    user = users.login_user('nouser@me.com', 'pwd1')
    assert user.name == 'nouser@me.com'
    assert user.api_key is not None
    user_id = auth.authenticate(user.api_key).identifier
    assert user.identifier == user_id
    # Activate the same user twice should not raise an error
    active_user = users.activate_user(user.identifier)
    assert active_user.identifier == user.identifier
    assert active_user.name == user.name
    assert active_user.api_key is None
    # Activate an unknown user will raise an error
    with pytest.raises(err.UnknownUserError):
        users.activate_user('UNK')


def test_list_users(tmpdir):
    """Test listing and searching for users."""
    # Create an inactive user.
    users, _ = init_db(str(tmpdir))
    # Register three active and one inactive user
    users.register_user('abc@me.com', 'pwd1', verify=False)
    users.register_user('ade@me.com', 'pwd1', verify=False)
    users.register_user('ABC@me.com', 'pwd1', verify=False)
    users.register_user('xyz@me.com', 'pwd1', verify=True)
    # Total listing contains the three active users
    assert len(users.list_users()) == 3
    # Query for different prefixes (ensures that LIKE is case sensitive)
    assert len(users.list_users(query='a')) == 2
    assert len(users.list_users(query='ab')) == 1
    assert len(users.list_users(query='abc')) == 1


def test_register_user(tmpdir):
    """Test registering a new user."""
    users, auth = init_db(str(tmpdir))
    reg_user = users.register_user('first.user@me.com', 'pwd1')
    assert reg_user.api_key is None
    user_1 = users.login_user('first.user@me.com', 'pwd1')
    assert user_1.name == 'first.user@me.com'
    user_id_1 = auth.authenticate(user_1.api_key).identifier
    assert user_1.identifier == user_id_1
    users.register_user('second.user@me.com', 'pwd2')
    user_2 = users.login_user('second.user@me.com', 'pwd2')
    assert user_2.name == 'second.user@me.com'
    user_id_2 = auth.authenticate(user_2.api_key).identifier
    assert user_2.identifier == user_id_2
    # Register user with existing email address raises error
    with pytest.raises(err.DuplicateUserError):
        users.register_user('first.user@me.com', 'pwd1')
    # Providing invalid email or passowrd will raise error
    with pytest.raises(err.ConstraintViolationError):
        users.register_user(None, 'pwd1')
    with pytest.raises(err.ConstraintViolationError):
        users.register_user(' \t', 'pwd1')
    with pytest.raises(err.ConstraintViolationError):
        users.register_user('a' * 513, 'pwd1')
    with pytest.raises(err.ConstraintViolationError):
        users.register_user('valid.name@me.com', ' ')


def test_reset_password(tmpdir):
    """Test resetting a user password."""
    users, auth = init_db(str(tmpdir))
    users.register_user('first.user@me.com', 'pwd1')
    user = users.login_user('first.user@me.com', 'pwd1')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key).identifier
    assert user.identifier == user_id
    request_id_1 = users.request_password_reset('first.user@me.com')
    assert request_id_1 is not None
    # Request reset for unknown user will return a reset request id
    request_id_2 = users.request_password_reset('unknown@me.com')
    assert request_id_2 is not None
    assert request_id_1 != request_id_2
    # Reset password for existing user
    user = users.reset_password(request_id=request_id_1, password='mypwd')
    assert user.identifier == user_id
    # After resetting the password the previous API key for the user is
    # invalid
    with pytest.raises(err.UnauthenticatedAccessError):
        auth.authenticate(user.api_key)
    user = users.login_user('first.user@me.com', 'mypwd')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key).identifier
    assert user.identifier == user_id
    # An error is raised when (i) trying to use a request for an unknown
    # user, (ii) a previously completed reset request, or (iii) an unknown
    # request identifier to reset a user password
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id_1, password='mypwd')
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id_2, password='mypwd')
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id='unknown', password='mypwd')


def test_reset_request_timeout(tmpdir):
    """Test resetting a user password using a request identifier that has
    timed out.
    """
    users, auth = init_db(str(tmpdir))
    users.register_user('first.user@me.com', 'pwd1')
    user = users.login_user('first.user@me.com', 'pwd1')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key).identifier
    assert user.identifier == user_id
    # Hack: Manipulate the login_timeout value to similate timeout
    users = UserManager(con=users.con, login_timeout=1)
    # Request reset and sleep for two seconds. This should allow the
    # request to timeout
    request_id = users.request_password_reset('first.user@me.com')
    time.sleep(2)
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id, password='mypwd')
