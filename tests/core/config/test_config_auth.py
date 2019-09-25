# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables that are used to configure the user manager.
"""

import os
import pytest

import robcore.config.auth as config
import robcore.error as err


class TestConfigAuth(object):
    """Test methods that get values from environment variables that are used to
    configure the user manager.
    """
    def test_config_auth(self):
        """Test public methods to get login timeout configuration."""
        # Clear environment variable values if set
        if config.ROB_AUTH_LOGINTTL in os.environ:
            del os.environ[config.ROB_AUTH_LOGINTTL]
        assert config.AUTH_LOGINTTL() == config.DEFAULT_LOGINTTL
        assert config.AUTH_LOGINTTL(default_value='XYZ') == config.DEFAULT_LOGINTTL
        assert config.AUTH_LOGINTTL(default_value=123) == 123
        with pytest.raises(err.MissingConfigurationError):
            assert config.AUTH_LOGINTTL(raise_error=True)
        os.environ[config.ROB_AUTH_LOGINTTL] = 'ABC'
        assert config.AUTH_LOGINTTL() == config.DEFAULT_LOGINTTL
        assert config.AUTH_LOGINTTL(default_value='XYZ') == config.DEFAULT_LOGINTTL
        os.environ[config.ROB_AUTH_LOGINTTL] = '345'
        assert config.AUTH_LOGINTTL() == 345
        assert config.AUTH_LOGINTTL(default_value='XYZ') == 345
