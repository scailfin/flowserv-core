# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables that are used to configure the user manager.
"""

import os

import flowserv.config.auth as config


def test_config_auth():
    """Test public methods to get login timeout configuration."""
    # -- Test get value with environment variable set ---------------------
    os.environ[config.FLOWSERV_AUTH_LOGINTTL] = '100'
    assert config.AUTH_LOGINTTL() == 100
    assert config.AUTH_LOGINTTL(value=123) == 123
    assert config.AUTH_LOGINTTL(value='ABC') == 'ABC'
    os.environ[config.FLOWSERV_AUTH_LOGINTTL] = 'ABC'
    assert config.AUTH_LOGINTTL() == config.DEFAULT_LOGINTTL
    # -- Test with environment variable not set ---------------------------
    del os.environ[config.FLOWSERV_AUTH_LOGINTTL]
    assert config.AUTH_LOGINTTL() == config.DEFAULT_LOGINTTL
    assert config.AUTH_LOGINTTL(value=123) == 123
    assert config.AUTH_LOGINTTL(value='ABC') == 'ABC'
