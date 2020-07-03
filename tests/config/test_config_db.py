# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables that are used to configure the connection to the underlying database.
"""

import os
import pytest

import flowserv.config.db as config
import flowserv.error as err


class TestConfigDatabase(object):
    """Test methods that get values from environment variables that are used to
    configure the database.
    """
    def test_config_database(self):
        """Test public methods to get database configuration."""
        # Clear environment variable values if set
        if config.FLOWSERV_DB in os.environ:
            del os.environ[config.FLOWSERV_DB]
        assert config.DB_CONNECT() is None
        with pytest.raises(err.MissingConfigurationError):
            assert config.DB_CONNECT(raise_error=True)
        os.environ[config.FLOWSERV_DB] = 'ABC'
        assert config.DB_CONNECT(raise_error=True) == 'ABC'
