# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the configuration module that access values for environment
variables that are used to configure the connection to the underlying database.
"""

import os
import pytest

import flowserv.config.database as config
import flowserv.error as err


def test_config_database():
    """Test public methods to get database configuration."""
    # -- Get value with environment variable set ------------------------------
    os.environ[config.FLOWSERV_DB] = 'XYZ'
    assert config.DB_CONNECT() == 'XYZ'
    assert config.DB_CONNECT(value='ABC') == 'ABC'
    # -- Get value if environment variable is not set -------------------------
    del os.environ[config.FLOWSERV_DB]
    with pytest.raises(err.MissingConfigurationError):
        assert config.DB_CONNECT()
    assert config.DB_CONNECT(value='ABC') == 'ABC'


def test_config_webapp():
    """Test getting the value for thw web app flag from the environment
    variable FLOWSERV_WEBAPP.
    """
    # -- Get value with environment variable set ------------------------------
    os.environ[config.FLOWSERV_WEBAPP] = 'False'
    assert not config.WEBAPP()
    os.environ[config.FLOWSERV_WEBAPP] = 'false'
    assert not config.WEBAPP()
    os.environ[config.FLOWSERV_WEBAPP] = 'True'
    assert config.WEBAPP()
    # -- Get value if environment variable is not set -------------------------
    del os.environ[config.FLOWSERV_WEBAPP]
    assert not config.WEBAPP()
    assert config.WEBAPP(value=True)
