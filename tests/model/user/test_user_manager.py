# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for registration and password reset in the user manager."""

import pytest
import time

from flowserv.model.user.auth import OpenAccessAuth
from flowserv.model.user.manager import UserManager

import flowserv.error as err


def init_db(db):
    """Initialize the database. Returns an instance of the user manager
    and the authentication manager.
    """
    return UserManager(db), OpenAccessAuth(db)


def test_activate_user(database):
    """Test creating inactive users and activating them."""
    # Create an inactive user.
    users, auth = init_db(database)
    user = users.register_user('nouser@me.com', 'pwd1', verify=True)
    # Attempt to login will raise error
    with pytest.raises(err.UnknownUserError):
        users.login_user('nouser@me.com', 'pwd1')
    # After activating the user login should succeed
    active_user = users.activate_user(user.user_id)
    assert active_user.user_id == user.user_id
    assert active_user.name == user.name
    assert active_user.api_key is None
    user = users.login_user('nouser@me.com', 'pwd1')
    assert user.name == 'nouser@me.com'
    assert user.api_key is not None
    user_id = auth.authenticate(user.api_key.value).user_id
    assert user.user_id == user_id
    # Activate the same user twice should not raise an error
    active_user = users.activate_user(user.user_id)
    assert active_user.user_id == user.user_id
    assert active_user.name == user.name
    assert active_user.api_key is not None
    # Activate an unknown user will raise an error
    with pytest.raises(err.UnknownUserError):
        users.activate_user('UNK')


def test_list_users(database):
    """Test listing and searching for users."""
    # Create an inactive user.
    users, _ = init_db(database)
    # Register three active and one inactive user
    users.register_user('abc@me.com', 'pwd1', verify=False)
    users.register_user('ade@me.com', 'pwd1', verify=False)
    users.register_user('abc@you-and-me.com', 'pwd1', verify=False)
    users.register_user('def@me.com', 'pwd1', verify=False)
    users.register_user('xyz@me.com', 'pwd1', verify=True)
    # Total listing contains the three active users
    assert len(users.list_users()) == 4
    # Query for different prefixes
    assert len(users.list_users(prefix='a')) == 3
    assert len(users.list_users(prefix='ab')) == 2
    assert len(users.list_users(prefix='ade')) == 1


def test_register_user(database):
    """Test registering a new user."""
    users, auth = init_db(database)
    reg_user = users.register_user('first.user@me.com', 'pwd1')
    assert reg_user.api_key is None
    user_1 = users.login_user('first.user@me.com', 'pwd1')
    assert user_1.name == 'first.user@me.com'
    user_id_1 = auth.authenticate(user_1.api_key.value).user_id
    assert user_1.user_id == user_id_1
    users.register_user('second.user@me.com', 'pwd2')
    user_2 = users.login_user('second.user@me.com', 'pwd2')
    assert user_2.name == 'second.user@me.com'
    user_id_2 = auth.authenticate(user_2.api_key.value).user_id
    assert user_2.user_id == user_id_2
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


def test_reset_password(database):
    """Test resetting a user password."""
    users, auth = init_db(database)
    users.register_user('first.user@me.com', 'pwd1')
    user = users.login_user('first.user@me.com', 'pwd1')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key.value).user_id
    assert user.user_id == user_id
    request_id_1 = users.request_password_reset('first.user@me.com')
    assert request_id_1 is not None
    # Request reset for unknown user will return a reset request id
    request_id_2 = users.request_password_reset('unknown@me.com')
    assert request_id_2 is not None
    assert request_id_1 != request_id_2
    # Reset password for existing user
    user_key = user.api_key.value
    user = users.reset_password(request_id=request_id_1, password='mypwd')
    assert user.user_id == user_id
    # After resetting the password the previous API key for the user is
    # invalid
    with pytest.raises(err.UnauthenticatedAccessError):
        auth.authenticate(user_key)
    user = users.login_user('first.user@me.com', 'mypwd')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key.value).user_id
    assert user.user_id == user_id
    # An error is raised when (i) trying to use a request for an unknown
    # user, (ii) a previously completed reset request, or (iii) an unknown
    # request identifier to reset a user password
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id_1, password='mypwd')
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id_2, password='mypwd')
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id='unknown', password='mypwd')


def test_reset_request_timeout(database):
    """Test resetting a user password using a request identifier that has
    timed out.
    """
    users, auth = init_db(database)
    users.register_user('first.user@me.com', 'pwd1')
    user = users.login_user('first.user@me.com', 'pwd1')
    assert user.name == 'first.user@me.com'
    user_id = auth.authenticate(user.api_key.value).user_id
    assert user.user_id == user_id
    # Hack: Manipulate the login_timeout value to similate timeout
    users = UserManager(db=database, login_timeout=1)
    # Request reset and sleep for two seconds. This should allow the
    # request to timeout
    request_id = users.request_password_reset('first.user@me.com')
    time.sleep(2)
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id, password='mypwd')
