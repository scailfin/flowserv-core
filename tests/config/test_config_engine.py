# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that creates an instance of the
workflow controller.
"""

import os
import pytest

from flowserv.controller.sync import SyncWorkflowEngine
from flowserv.tests.benchmark import StateEngine

import flowserv.config.engine as config
import flowserv.core.error as err


class TestConfigEngine(object):
    """Unit test for method that creates an instance of the workflow controller
    from the respective environment variables.
    """
    def test_get_default_engine(self):
        """Test method to get the default workflow controller."""
        # Clear environment variable if set
        if config.FLOWSERV_ENGINE_CLASS in os.environ:
            del os.environ[config.FLOWSERV_ENGINE_CLASS]
        if config.FLOWSERV_ENGINE_MODULE in os.environ:
            del os.environ[config.FLOWSERV_ENGINE_MODULE]
        controller = config.FLOWSERV_ENGINE()
        assert isinstance(controller, SyncWorkflowEngine)

    def test_get_state_engine(self):
        """Test method to get an instance of the test engine using the
        environment variables."""
        # Set environment variables
        os.environ[config.FLOWSERV_ENGINE_MODULE] = 'flowserv.tests.benchmark'
        os.environ[config.FLOWSERV_ENGINE_CLASS] = 'StateEngine'
        controller = config.FLOWSERV_ENGINE()
        assert isinstance(controller, StateEngine)

    def test_invalid_config(self):
        """Test error cases where only one of the two environment variables
        is set.
        """
        # Clear environment variable 'FLOWSERV_ENGINE_CLASS' if set
        if config.FLOWSERV_ENGINE_CLASS in os.environ:
            del os.environ[config.FLOWSERV_ENGINE_CLASS]
        os.environ[config.FLOWSERV_ENGINE_MODULE] = 'module'
        with pytest.raises(err.MissingConfigurationError):
            config.FLOWSERV_ENGINE()
        del os.environ[config.FLOWSERV_ENGINE_MODULE]
        os.environ[config.FLOWSERV_ENGINE_CLASS] = 'class'
        with pytest.raises(err.MissingConfigurationError):
            config.FLOWSERV_ENGINE()
