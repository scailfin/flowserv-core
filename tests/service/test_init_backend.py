# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for function in the service module that creates an instance of
the workflow controller.
"""

import pytest

from flowserv.config import Config, FLOWSERV_BACKEND_CLASS, FLOWSERV_BACKEND_MODULE
from flowserv.controller.serial.docker import DockerWorkflowEngine
from flowserv.controller.serial.engine import SerialWorkflowEngine
from flowserv.service.backend import init_backend

import flowserv.error as err


def test_get_default_backend():
    """Test method to get the default workflow controller."""
    controller = init_backend(config=Config().basedir('/dev/null'))
    assert isinstance(controller, SerialWorkflowEngine)


def test_get_docker_backend():
    """Test method to instantiate the docker workflow controller."""
    controller = init_backend(config=Config().basedir('/dev/null').docker_engine())
    assert isinstance(controller, DockerWorkflowEngine)


def test_invalid_config():
    """Test error case for invlid configuration."""
    # Missing base directory
    with pytest.raises(err.MissingConfigurationError):
        init_backend(config=Config().multiprocess_engine())
    # Missing value for variable 'FLOWSERV_BACKEND_CLASS'
    config = Config().basedir('/dev/null').multiprocess_engine()
    del config[FLOWSERV_BACKEND_CLASS]
    with pytest.raises(err.MissingConfigurationError):
        init_backend(config=config)
    # Missing value for variable 'FLOWSERV_BACKEND_MODULE'
    config = Config().basedir('/dev/null').multiprocess_engine()
    del config[FLOWSERV_BACKEND_MODULE]
    with pytest.raises(err.MissingConfigurationError):
        init_backend(config=config)
