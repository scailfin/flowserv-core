# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the CLI environment context object."""

import os
import pytest

from flowserv.client.cli.base import EnvContext

import flowserv.config as config
import flowserv.error as err


@pytest.fixture
def context():
    """Get context object for flowserv CLI."""
    return EnvContext(
        vars={'workflow': config.FLOWSERV_APP, 'group': config.FLOWSERV_GROUP}
    )


def test_env_app_identifier(context):
    """Test getting the workflow identifier and sbmission identifier from the
    environment.
    """
    os.environ[config.FLOWSERV_APP] = '0000'
    assert context.get_workflow(dict()) == '0000'
    assert context.get_group(dict()) == '0000'
    os.environ[config.FLOWSERV_GROUP] = '000A'
    assert context.get_workflow(dict()) == '0000'
    assert context.get_group(dict()) == '000A'
    del os.environ[config.FLOWSERV_APP]
    del os.environ[config.FLOWSERV_GROUP]
    with pytest.raises(err.MissingConfigurationError):
        context.get_workflow(dict())
    with pytest.raises(err.MissingConfigurationError):
        context.get_group(dict())


def test_env_access_token(context):
    """Test getting the access token from the environment."""
    os.environ[config.FLOWSERV_ACCESS_TOKEN] = '0001'
    assert context.access_token() == '0001'
    del os.environ[config.FLOWSERV_ACCESS_TOKEN]
    with pytest.raises(err.MissingConfigurationError):
        context.access_token()
