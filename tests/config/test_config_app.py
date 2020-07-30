# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for application configuration."""

import os

from flowserv.config.app import APP_KEY, FLOWSERV_APP, SYNC
from flowserv.config.controller import ENGINE_ASYNC, FLOWSERV_ASYNC


def test_app_key():
    """Test getting application key from the environment."""
    os.environ[FLOWSERV_APP] = '0000'
    assert APP_KEY() == '0000'
    assert APP_KEY(value='0001') == '0001'
    del os.environ[FLOWSERV_APP]


def test_engine_sync():
    """Test setting the flowserv engine's async environment variable."""
    os.environ[FLOWSERV_ASYNC] = 'True'
    assert ENGINE_ASYNC()
    SYNC()
    assert not ENGINE_ASYNC()
    del os.environ[FLOWSERV_ASYNC]
