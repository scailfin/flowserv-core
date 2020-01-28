# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for function in the service module that creates an instance of
the workflow controller.
"""

import os
import pytest

from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.tests.controller import StateEngine

import flowserv.config.backend as config
import flowserv.core.error as err
import flowserv.service.backend as service


def test_get_default_backend():
    """Test method to get the default workflow controller."""
    # Clear environment variable if set
    if config.FLOWSERV_BACKEND_CLASS in os.environ:
        del os.environ[config.FLOWSERV_BACKEND_CLASS]
    if config.FLOWSERV_BACKEND_MODULE in os.environ:
        del os.environ[config.FLOWSERV_BACKEND_MODULE]
    controller = service.init_backend()
    assert isinstance(controller, SerialWorkflowEngine)


def test_get_state_engine():
    """Test method to get an instance of the test engine using the
    environment variables."""
    # Set environment variables
    os.environ[config.FLOWSERV_BACKEND_MODULE] = 'flowserv.tests.controller'
    os.environ[config.FLOWSERV_BACKEND_CLASS] = 'StateEngine'
    controller = service.init_backend()
    assert isinstance(controller, StateEngine)


def test_invalid_config():
    """Test error cases where only one of the two environment variables
    is set.
    """
    # Clear environment variable 'FLOWSERV_BACKEND_CLASS' if set
    if config.FLOWSERV_BACKEND_CLASS in os.environ:
        del os.environ[config.FLOWSERV_BACKEND_CLASS]
    os.environ[config.FLOWSERV_BACKEND_MODULE] = 'module'
    with pytest.raises(err.MissingConfigurationError):
        service.init_backend()
    del os.environ[config.FLOWSERV_BACKEND_MODULE]
    os.environ[config.FLOWSERV_BACKEND_CLASS] = 'class'
    with pytest.raises(err.MissingConfigurationError):
        service.init_backend()
