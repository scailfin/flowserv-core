# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the user resources view."""

from flowserv.model.user import UserManager
from flowserv.view.user import UserSerializer
from flowserv.view.validate import validator

import flowserv.view.user as labels


def test_password_request_serialization():
    """Test serialization of the password reset request response."""
    schema = validator('PasswordResetResponse')
    doc = UserSerializer().reset_request('0000')
    schema.validate(doc)
    assert doc[labels.REQUEST_ID] == '0000'


def test_user_handle_serialization(database):
    """Test serialization of user handles."""
    schema = validator('User')
    view = UserSerializer()
    with database.session() as session:
        manager = UserManager(session)
        user = manager.register_user('alice', 'mypwd')
        doc = view.user(user)
        schema.validate(doc)
        assert doc[labels.USER_NAME] == 'alice'
        assert labels.USER_TOKEN not in doc
        user = manager.login_user('alice', 'mypwd')
        doc = view.user(user, include_token=True)
        schema.validate(doc)
        assert doc[labels.USER_NAME] == 'alice'
        assert labels.USER_TOKEN in doc


def test_user_listing_serialization(database):
    """Test serialization of user listings."""
    schema = validator('UserListing')
    view = UserSerializer()
    with database.session() as session:
        manager = UserManager(session)
        manager.register_user('alice', 'mypwd')
        manager.register_user('bob', 'mypwd')
        doc = view.user_listing(manager.list_users())
        schema.validate(doc)
        assert len(doc[labels.USER_LIST]) == 2
