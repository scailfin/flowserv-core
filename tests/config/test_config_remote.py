# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for getting the polling interval for remote workflow monitors."""

import os

import flowserv.controller.remote.config as config


def test_config_poll_interval():
    """Test getting the polling interval value from the configuration."""
    # -- Test get value with environment variable set ---------------------
    os.environ[config.FLOWSERV_POLL_INTERVAL] = '100'
    assert config.POLL_INTERVAL() == 100
    assert config.POLL_INTERVAL(value=123) == 123
    assert config.POLL_INTERVAL(value='ABC') == 'ABC'
    os.environ[config.FLOWSERV_POLL_INTERVAL] = 'ABC'
    assert config.POLL_INTERVAL() == config.DEFAULT_POLL_INTERVAL
    # -- Test with environment variable not set ---------------------------
    del os.environ[config.FLOWSERV_POLL_INTERVAL]
    assert config.POLL_INTERVAL() == config.DEFAULT_POLL_INTERVAL
    assert config.POLL_INTERVAL(value=123) == 123
    assert config.POLL_INTERVAL(value='ABC') == 'ABC'
