# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the controller configuration."""

import os

import flowserv.config.controller as config


def test_config_async_engine():
    """Test getting the value for the engine async flag from the environment.
    """
    # -- Test with environment variable set -----------------------------------
    os.environ[config.FLOWSERV_ASYNC] = 'True'
    assert config.ENGINE_ASYNC()
    os.environ[config.FLOWSERV_ASYNC] = 'False'
    assert not config.ENGINE_ASYNC()
    os.environ[config.FLOWSERV_ASYNC] = '1'
    assert not config.ENGINE_ASYNC()
    # -- Test with environment variable not set -------------------------------
    del os.environ[config.FLOWSERV_ASYNC]
    assert config.ENGINE_ASYNC()
