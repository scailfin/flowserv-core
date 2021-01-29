# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for API authentication methods."""

import flowserv.config as config


def test_api_authenticate(local_service):
    """Test methods to authenticate user via the API factory."""
    # -- Setup (create user) --------------------------------------------------
    # Delete access token if exists.
    if config.FLOWSERV_ACCESS_TOKEN in local_service:
        del local_service[config.FLOWSERV_ACCESS_TOKEN]
    # Create user alice.
    with local_service() as api:
        api.users().register_user(username='alice', password='mypwd', verify=False)
    # -- Login and logout -----------------------------------------------------
    local_service.login(username='alice', password='mypwd')
    assert local_service[config.FLOWSERV_ACCESS_TOKEN] is not None
    local_service.logout()
    assert config.FLOWSERV_ACCESS_TOKEN not in local_service
