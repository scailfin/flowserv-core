# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the local user service API."""

import pytest

import flowserv.error as err
import flowserv.tests.serialize as serialize


def test_authenticate_user_local(local_service):
    """Test login and logout via API."""
    # -- Register a new user that is automatically activated ------------------
    with local_service() as api:
        r = api.users().register_user(
            username='myuser',
            password='mypwd',
            verify=False
        )
        serialize.validate_user_handle(doc=r, login=False)
    # -- Login ----------------------------------------------------------------
    with local_service() as api:
        r = api.users().login_user(username='myuser', password='mypwd')
        serialize.validate_user_handle(doc=r, login=True)
        access_token = r['token']
    # -- Who am I -------------------------------------------------------------
    with local_service() as api:
        r = api.users().whoami_user(access_token)
        serialize.validate_user_handle(doc=r, login=True)
    # -- Logout ---------------------------------------------------------------
    with local_service() as api:
        r = api.users().logout_user(access_token)
        serialize.validate_user_handle(doc=r, login=False)


def test_list_users_local(local_service):
    """Test user listings and queries."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create three active user.
    with local_service() as api:
        users = api.users()
        # Register three active users
        users.register_user(username='a@user', password='mypwd', verify=False)
        users.register_user(username='me@user', password='mypwd', verify=False)
        users.register_user(username='my@user', password='mypwd', verify=False)
    # -- List all user --------------------------------------------------------
    with local_service() as api:
        r = api.users().list_users()
        serialize.validate_user_listing(r)
        assert len(r['users']) == 3
    # -- Query users ----------------------------------------------------------
    with local_service() as api:
        r = api.users().list_users(query='m')
        serialize.validate_user_listing(r)
        assert len(r['users']) == 2
        r = api.users().list_users(query='a')
        serialize.validate_user_listing(r)
        assert len(r['users']) == 1


def test_register_user_local(local_service):
    """Test new user registration via API."""
    # -- Register a new user without activating the user ----------------------
    with local_service() as api:
        r = api.users().register_user(
            username='myuser',
            password='mypwd',
            verify=True
        )
        serialize.validate_user_handle(doc=r, login=False, inactive=True)
    # -- Activate the user ----------------------------------------------------
    with local_service() as api:
        r = api.users().activate_user(r['id'])
        serialize.validate_user_handle(doc=r, login=False)
        # Register a new user that is automatically activated
        r = api.users().register_user(
            username='myuser2',
            password='mypwd',
            verify=False
        )
        serialize.validate_user_handle(doc=r, login=False)


def test_reset_password_local(local_service):
    """Test requesting a reset and resetting the password for a user."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one active user.
    username = 'myuser'
    pwd = 'mypwd'
    with local_service() as api:
        api.users().register_user(
            username=username,
            password=pwd,
            verify=False
        )
    # -- Request password reset -----------------------------------------------
    with local_service() as api:
        r = api.users().request_password_reset(username)
        serialize.validate_reset_request(r)
    # -- Update the user password ---------------------------------------------
    with local_service() as api:
        newpwd = 'abcde'
        request_id = r['requestId']
        r = api.users().reset_password(request_id=request_id, password=newpwd)
        serialize.validate_user_handle(doc=r, login=False)
    # -- Error when using invalid request identifier --------------------------
    with local_service() as api:
        with pytest.raises(err.UnknownRequestError):
            api.users().reset_password(request_id=request_id, password=newpwd)
