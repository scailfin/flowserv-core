# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables that are used to configure the API.
"""

import os
import pytest

import flowserv.config.api as config
import flowserv.core.error as err


class TestConfigAPI(object):
    """Test methods that get values from environment variables that are used to
    configure the API.
    """
    def test_config_basedir(self):
        """Test method to get the API base directory."""
        # Clear environment variable if set
        if config.FLOWSERV_API_BASEDIR in os.environ:
            del os.environ[config.FLOWSERV_API_BASEDIR]
        assert config.API_BASEDIR() == config.DEFAULT_DIR
        assert config.API_BASEDIR(default_value='XYZ') == 'XYZ'
        with pytest.raises(err.MissingConfigurationError):
            assert config.API_BASEDIR(raise_error=True)
        os.environ[config.FLOWSERV_API_BASEDIR] = 'ABC'
        assert config.API_BASEDIR(default_value='XYZ') == 'ABC'
        # Cleanup
        del os.environ[config.FLOWSERV_API_BASEDIR]

    def test_config_host(self):
        """Test method to get the API server host name."""
        # Clear environment variable if set
        if config.FLOWSERV_API_HOST in os.environ:
            del os.environ[config.FLOWSERV_API_HOST]
        assert config.API_HOST() == config.DEFAULT_HOST
        os.environ[config.FLOWSERV_API_HOST] = 'my.host.com'
        assert config.API_HOST() == 'my.host.com'

    def test_config_name(self):
        """Test method to get the API name."""
        # Clear environment variable if set
        if config.FLOWSERV_API_NAME in os.environ:
            del os.environ[config.FLOWSERV_API_NAME]
        assert config.API_NAME() == config.DEFAULT_NAME
        os.environ[config.FLOWSERV_API_NAME] = 'ABC'
        assert config.API_NAME() == 'ABC'

    def test_config_path(self):
        """Test method to get the API application path."""
        # Clear environment variable if set
        if config.FLOWSERV_API_PATH in os.environ:
            del os.environ[config.FLOWSERV_API_PATH]
        assert config.API_PATH() == config.DEFAULT_PATH
        os.environ[config.FLOWSERV_API_PATH] = 'api-path'
        assert config.API_PATH() == 'api-path'

    def test_config_port(self):
        """Test method to get the API port number."""
        # Clear environment variable if set
        if config.FLOWSERV_API_PORT in os.environ:
            del os.environ[config.FLOWSERV_API_PORT]
        assert config.API_PORT() == config.DEFAULT_PORT
        os.environ[config.FLOWSERV_API_PORT] = 'ABC'
        with pytest.raises(ValueError):
            config.API_PORT()
        os.environ[config.FLOWSERV_API_PORT] = '5005'
        assert config.API_PORT() == 5005

    def test_config_url(self):
        """Test method to get the API base URL."""
        # Clear environment variable if set
        if config.FLOWSERV_API_HOST in os.environ:
            del os.environ[config.FLOWSERV_API_HOST]
        if config.FLOWSERV_API_PATH in os.environ:
            del os.environ[config.FLOWSERV_API_PATH]
        if config.FLOWSERV_API_PORT in os.environ:
            del os.environ[config.FLOWSERV_API_PORT]
        default_url = '{}:{}{}'.format(
            config.DEFAULT_HOST,
            config.DEFAULT_PORT,
            config.DEFAULT_PATH
        )
        assert config.API_URL() == default_url
        os.environ[config.FLOWSERV_API_PORT] = '80'
        os.environ[config.FLOWSERV_API_PATH] = 'app-path/v1'
        assert config.API_URL() == config.DEFAULT_HOST + '/app-path/v1'
