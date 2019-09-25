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

import robcore.config.api as config
import robcore.error as err


class TestConfigAPI(object):
    """Test methods that get values from environment variables that are used to
    configure the API.
    """
    def test_config_basedir(self):
        """Test method to get the API base directory."""
        # Clear environment variable if set
        if config.ROB_API_BASEDIR in os.environ:
            del os.environ[config.ROB_API_BASEDIR]
        assert config.API_BASEDIR() == config.DEFAULT_DIR
        assert config.API_BASEDIR(default_value='XYZ') == 'XYZ'
        with pytest.raises(err.MissingConfigurationError):
            assert config.API_BASEDIR(raise_error=True)
        os.environ[config.ROB_API_BASEDIR] = 'ABC'
        assert config.API_BASEDIR(default_value='XYZ') == 'ABC'

    def test_config_name(self):
        """Test method to get the API name."""
        # Clear environment variable if set
        if config.ROB_API_NAME in os.environ:
            del os.environ[config.ROB_API_NAME]
        assert config.API_NAME() == config.DEFAULT_NAME
        assert config.API_NAME(default_value='XYZ') == 'XYZ'
        with pytest.raises(err.MissingConfigurationError):
            assert config.API_NAME(raise_error=True)
        os.environ[config.ROB_API_NAME] = 'ABC'
        assert config.API_NAME(default_value='XYZ') == 'ABC'

    def test_config_url(self):
        """Test method to get the API base URL."""
        # Clear environment variable if set
        if config.ROB_API_URL in os.environ:
            del os.environ[config.ROB_API_URL]
        assert config.API_URL() == config.DEFAULT_URL
        assert config.API_URL(default_value='XYZ') == 'XYZ'
        with pytest.raises(err.MissingConfigurationError):
            assert config.API_URL(raise_error=True)
        os.environ[config.ROB_API_URL] = 'ABC'
        assert config.API_URL(default_value='XYZ') == 'ABC'
