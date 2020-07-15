# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that access values for environment
variables.
"""

import os
import pytest

import flowserv.config.base as config
import flowserv.error as err


def test_get_variable_value():
    """Test internal method to access environment variables."""
    # Set environment variable 'FLOWSERV_TEST'
    test_value = 'TestValue'
    os.environ[config.FLOWSERV_TEST] = test_value
    assert config.get_variable(config.FLOWSERV_TEST, None, False) == test_value
    assert config.get_variable(config.FLOWSERV_TEST, 'V', False) == test_value
    assert config.get_variable(config.FLOWSERV_TEST, None, True) == test_value
    assert config.get_variable(config.FLOWSERV_TEST, 'V', True) == test_value
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
