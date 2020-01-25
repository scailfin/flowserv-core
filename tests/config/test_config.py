# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables.
"""

import os
import pytest

import flowserv.config.base as config
import flowserv.core.error as err


class TestConfig(object):
    """Test methods that get configuration values from environment variables.
    """
    def test_get_variable_value(self):
        """Test internal method to access environment variables."""
        # Set environment variable 'FLOWSERV_TEST'
        os.environ[config.FLOWSERV_TEST] = 'TestValue'
        assert config.get_variable(config.FLOWSERV_TEST, None, False) == 'TestValue'
        assert config.get_variable(config.FLOWSERV_TEST, 'V', False) == 'TestValue'
        assert config.get_variable(config.FLOWSERV_TEST, None, True) == 'TestValue'
        assert config.get_variable(config.FLOWSERV_TEST, 'V', True) == 'TestValue'
        # Set variable to None
        del os.environ[config.FLOWSERV_TEST]
        assert config.get_variable(config.FLOWSERV_TEST, None, False) is None
        assert config.get_variable(config.FLOWSERV_TEST, 'V', False) == 'V'
        with pytest.raises(err.MissingConfigurationError):
            assert config.get_variable(config.FLOWSERV_TEST, None, True)
        with pytest.raises(err.MissingConfigurationError):
            assert config.get_variable(config.FLOWSERV_TEST, 'V', True)
        # Set variable to empty string
        os.environ[config.FLOWSERV_TEST] = ''
        assert config.get_variable(config.FLOWSERV_TEST, None, False) is None
        assert config.get_variable(config.FLOWSERV_TEST, 'V', False) == 'V'
        with pytest.raises(err.MissingConfigurationError):
            assert config.get_variable(config.FLOWSERV_TEST, None, True)
        with pytest.raises(err.MissingConfigurationError):
            assert config.get_variable(config.FLOWSERV_TEST, 'V', True)
