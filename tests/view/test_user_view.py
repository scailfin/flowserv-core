# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test API methods for user resources and login/logout."""

import pytest

from flowserv.model.user.auth import OpenAccessAuth

import flowserv.view.hateoas as hateoas
import flowserv.view.labels as labels
import flowserv.core.error as err
import flowserv.tests.api as api
import flowserv.tests.serialize as serialize
import flowserv.core.util as util


"""Mandatory labels in a user handle for users that are currently logged in or
logged out.
"""
USER_LOGIN = [labels.ID, labels.USERNAME, labels.ACCESS_TOKEN, labels.LINKS]
USER_LOGOUT = [labels.ID, labels.USERNAME, labels.LINKS]


class TestUserView(object):
    """Test API methods that access and manipulate users."""
    def test_authenticate_user(self, tmpdir):
        """Test login and logout via API."""
        basedir = str(tmpdir)
        _, _, users, _, auth, _, _ = api.init_api(basedir, open_access=True)
        # Register a new user that is automatically activated
        users.register_user(username='myuser', password='mypwd', verify=False)
        # Login
        r = users.login_user(username='myuser', password='mypwd')
        util.validate_doc(doc=r, mandatory_labels=USER_LOGIN)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.WHOAMI,
                hateoas.action(hateoas.LOGOUT)
            ]
        )
        access_token = r[labels.ACCESS_TOKEN]
        r = users.whoami_user(auth.authenticate(access_token))
        util.validate_doc(doc=r, mandatory_labels=USER_LOGIN)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.WHOAMI,
                hateoas.action(hateoas.LOGOUT)
            ]
        )
        # Logout
        r = users.logout_user(auth.authenticate(access_token))
        util.validate_doc(doc=r, mandatory_labels=USER_LOGOUT)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.action(hateoas.LOGIN)
            ]
        )

    def test_list_users(self, tmpdir):
        """Test user listings and queries."""
        _, _, users, _, _, _, _ = api.init_api(str(tmpdir), open_access=True)
        # Register three active users
        users.register_user(username='a@user', password='mypwd', verify=False)
        users.register_user(username='me@user', password='mypwd', verify=False)
        users.register_user(username='my@user', password='mypwd', verify=False)
        r = users.list_users()
        util.validate_doc(doc=r, mandatory_labels=[labels.USERS, labels.LINKS])
        serialize.validate_links(doc=r, keys=[hateoas.SELF])
        assert len(r[labels.USERS]) == 3
        for u in r[labels.USERS]:
            util.validate_doc(
                doc=u,
                mandatory_labels=[labels.ID, labels.USERNAME]
            )
        r = users.list_users(query='m')
        util.validate_doc(doc=r, mandatory_labels=[labels.USERS, labels.LINKS])
        assert len(r[labels.USERS]) == 2
        r = users.list_users(query='a')
        util.validate_doc(doc=r, mandatory_labels=[labels.USERS, labels.LINKS])
        assert len(r[labels.USERS]) == 1

    def test_register_user(self, tmpdir):
        """Test new user registration via API."""
        _, _, users, _, _, _, _ = api.init_api(str(tmpdir), open_access=True)
        # Register a new user without activating the user
        r = users.register_user(
            username='myuser',
            password='mypwd',
            verify=True
        )
        util.validate_doc(doc=r, mandatory_labels=USER_LOGOUT)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.action(hateoas.LOGIN),
                hateoas.action(hateoas.ACTIVATE)
            ]
        )
        # Activate the user
        r = users.activate_user(r[labels.ID])
        util.validate_doc(doc=r, mandatory_labels=USER_LOGOUT)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.action(hateoas.LOGIN)
            ]
        )
        # Register a new user that is automatically activated
        r = users.register_user(
            username='myuser2',
            password='mypwd',
            verify=False
        )
        util.validate_doc(doc=r, mandatory_labels=USER_LOGOUT)
        links = hateoas.deserialize(r[labels.LINKS])
        util.validate_doc(
            doc=links,
            mandatory_labels=[
                hateoas.action(hateoas.LOGIN)
            ]
        )

    def test_reset_password(self, tmpdir):
        """Test requesting a reset and resetting the password for a user."""
        _, _, users, _, _, _, _ = api.init_api(str(tmpdir), open_access=True)
        # Register a new user
        users.register_user(username='myuser', password='mypwd', verify=False)
        # Request password reset
        r = users.request_password_reset('myuser')
        util.validate_doc(r, mandatory_labels=[labels.REQUEST_ID])
        # Update the user password
        request_id = r[labels.REQUEST_ID]
        r = users.reset_password(request_id=request_id, password='abcde')
        util.validate_doc(doc=r, mandatory_labels=USER_LOGOUT)
        # Error when using invalid request identifier
        with pytest.raises(err.UnknownRequestError):
            users.reset_password(request_id=request_id, password='abcde')
