# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for the remote user service API."""

from flowserv.view.user import USER_TOKEN


def test_activate_user_remote(remote_service, mock_response):
    """Test registering a user at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().activate_user(user_id='0000')


def test_list_user_remote(remote_service, mock_response):
    """Test list users from the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().list_users()


def test_login_user_remote(remote_service, mock_response):
    """Test login users at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    r = remote_service.users().login_user(
        username='myuser',
        password='mypwd'
    )
    assert r == {USER_TOKEN: '0000'}


def test_logout_user_remote(remote_service, mock_response):
    """Test logout users at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().logout_user(api_key='0000')


def test_request_password_reset_remote(remote_service, mock_response):
    """Test requesting a password reset for a user at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().request_password_reset(username='myuser')


def test_register_user_remote(remote_service, mock_response):
    """Test registering a user at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().register_user(
        username='myuser',
        password='mypwd',
        verify=False
    )


def test_reset_password_remote(remote_service, mock_response):
    """Test resetting a user at the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().reset_password(
        request_id='0000',
        password='mypwd'
    )


def test_whoami_user_remote(remote_service, mock_response):
    """Test get information about logged in user from the remote server."""
    # -- Register a new user that is automatically activated ------------------
    remote_service.users().whoami_user(api_key='0000')
