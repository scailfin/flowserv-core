# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Test methods of the configuration module that creates an instance of the
workflow controller.
"""

import os
import pytest

from robcore.controller.backend.sync import SyncWorkflowEngine
from robcore.tests.benchmark import StateEngine

import robcore.config.engine as config
import robcore.error as err


class TestConfigEngine(object):
    """Unit test for method that creates an instance of the workflow controller
    from the respective environment variables.
    """
    def test_get_default_engine(self):
        """Test method to get the default workflow controller."""
        # Clear environment variable if set
        if config.ROB_ENGINE_CLASS in os.environ:
            del os.environ[config.ROB_ENGINE_CLASS]
        if config.ROB_ENGINE_MODULE in os.environ:
            del os.environ[config.ROB_ENGINE_MODULE]
        controller = config.ROB_ENGINE()
        assert isinstance(controller, SyncWorkflowEngine)

    def test_get_state_engine(self):
        """Test method to get an instance of the test engine using the
        environment variables."""
        # Set environment variables
        os.environ[config.ROB_ENGINE_MODULE] = 'robcore.tests.benchmark'
        os.environ[config.ROB_ENGINE_CLASS] = 'StateEngine'
        controller = config.ROB_ENGINE()
        assert isinstance(controller, StateEngine)

    def test_invalid_config(self):
        """Test error cases where only one of the two environment variables
        is set.
        """
        # Clear environment variable 'ROB_ENGINE_CLASS' if set
        if config.ROB_ENGINE_CLASS in os.environ:
            del os.environ[config.ROB_ENGINE_CLASS]
        os.environ[config.ROB_ENGINE_MODULE] = 'module'
        with pytest.raises(err.MissingConfigurationError):
            config.ROB_ENGINE()
        del os.environ[config.ROB_ENGINE_MODULE]
        os.environ[config.ROB_ENGINE_CLASS] = 'class'
        with pytest.raises(err.MissingConfigurationError):
            config.ROB_ENGINE()
