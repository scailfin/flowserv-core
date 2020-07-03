# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API class that access and manipulate user resources and that
enable users to login and logout.
"""

import pytest

from flowserv.service.api import API

import flowserv.error as err
import flowserv.tests.db as db
import flowserv.tests.serialize as serialize


def test_authenticate_user(tmpdir):
    """Test login and logout via API."""
    api = API(con=db.init_db(str(tmpdir)).connect(), basedir=str(tmpdir))
    users = api.users()
    # Register a new user that is automatically activated
    r = users.register_user(username='myuser', password='mypwd', verify=False)
    serialize.validate_user_handle(doc=r, login=False)
    # Login
    r = users.login_user(username='myuser', password='mypwd')
    serialize.validate_user_handle(doc=r, login=True)
    access_token = r['token']
    r = users.whoami_user(api.auth.authenticate(access_token))
    serialize.validate_user_handle(doc=r, login=True)
    # Logout
    r = users.logout_user(api.auth.authenticate(access_token))
    serialize.validate_user_handle(doc=r, login=False)


def test_list_users(tmpdir):
    """Test user listings and queries."""
    api = API(con=db.init_db(str(tmpdir)).connect(), basedir=str(tmpdir))
    users = api.users()
    # Register three active users
    users.register_user(username='a@user', password='mypwd', verify=False)
    users.register_user(username='me@user', password='mypwd', verify=False)
    users.register_user(username='my@user', password='mypwd', verify=False)
    r = users.list_users()
    serialize.validate_user_listing(r)
    assert len(r['users']) == 3
    r = users.list_users(query='m')
    serialize.validate_user_listing(r)
    assert len(r['users']) == 2
    r = users.list_users(query='a')
    serialize.validate_user_listing(r)
    assert len(r['users']) == 1


def test_register_user(tmpdir):
    """Test new user registration via API."""
    api = API(con=db.init_db(str(tmpdir)).connect(), basedir=str(tmpdir))
    users = api.users()
    # Register a new user without activating the user
    r = users.register_user(
        username='myuser',
        password='mypwd',
        verify=True
    )
    serialize.validate_user_handle(doc=r, login=False, inactive=True)
    # Activate the user
    r = users.activate_user(r['id'])
    serialize.validate_user_handle(doc=r, login=False)
    # Register a new user that is automatically activated
    r = users.register_user(
        username='myuser2',
        password='mypwd',
        verify=False
    )
    serialize.validate_user_handle(doc=r, login=False)


def test_reset_password(tmpdir):
    """Test requesting a reset and resetting the password for a user."""
    api = API(con=db.init_db(str(tmpdir)).connect(), basedir=str(tmpdir))
    users = api.users()
    # Register a new user
    users.register_user(username='myuser', password='mypwd', verify=False)
    # Request password reset
    r = users.request_password_reset('myuser')
    serialize.validate_reset_request(r)
    # Update the user password
    request_id = r['requestId']
    r = users.reset_password(request_id=request_id, password='abcde')
    serialize.validate_user_handle(doc=r, login=False)
    # Error when using invalid request identifier
    with pytest.raises(err.UnknownRequestError):
        users.reset_password(request_id=request_id, password='abcde')
