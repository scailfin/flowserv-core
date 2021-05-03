# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test to validate that login tokens timeout after a specified time
period.
"""

import pytest
import time

from flowserv.model.auth import DefaultAuthPolicy, OpenAccessAuth
from flowserv.model.user import UserManager

import flowserv.error as err
import flowserv.tests.model as model


@pytest.mark.parametrize('authcls', [DefaultAuthPolicy, OpenAccessAuth])
def test_authenticate_after_timeout(database, authcls):
    """Test authentication after key expired."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with a single active user.
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
    # Test authenticate after timeout -----------------------------------------
    with database.session() as session:
        # Create user manager with TTL for login tokens of 1 sec.
        users = UserManager(session, token_timeout=1)
        auth = authcls(session)
        # Authenticate user 1. Then sleep for 1.5 sec. Trying to authenticate
        # the user after the sleep period should raise an error.
        api_key = users.login_user(user_1, user_1).api_key.value
        time.sleep(1.5)
        with pytest.raises(err.UnauthenticatedAccessError):
            auth.authenticate(api_key)


@pytest.mark.parametrize('authcls', [DefaultAuthPolicy, OpenAccessAuth])
def test_login_after_timeout(database, authcls):
    """Test login after key expired."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with a single active user.
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
    # Test login after timeout -----------------------------------------
    with database.session() as session:
        # Create user manager with TTL for login tokens of 1 sec.
        users = UserManager(session, token_timeout=1)
        # Authenticate user 1. Then sleep for 1.5 sec. When logging in again an
        # new api-key is generated.
        api_key = users.login_user(user_1, user_1).api_key.value
        time.sleep(1.5)
        assert users.login_user(user_1, user_1).api_key.value != api_key
