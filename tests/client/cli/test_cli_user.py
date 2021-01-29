# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the user command-line interface."""

import os

from flowserv.client.cli.base import cli_flowserv as cli

import flowserv.config as config


def test_create_user(flowserv_cli):
    """Test registering and authenticating a user."""
    # -- Register user Alice --------------------------------------------------
    cmd = ['users', 'register', '-u', 'alice', '-p', 'abc123']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Login as Alice -------------------------------------------------------
    cmd = ['login', '-u', 'alice', '-p', 'abc123']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    access_token = result.output[result.output.rfind('=') + 1:].strip()
    os.environ[config.FLOWSERV_ACCESS_TOKEN] = access_token
    # -- Who am I -------------------------------------------------------------
    cmd = ['whoami']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'alice' in result.output
    # -- Logout ---------------------------------------------------------------
    cmd = ['logout']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Cleanup --------------------------------------------------------------
    del os.environ[config.FLOWSERV_ACCESS_TOKEN]


def test_list_users(flowserv_cli):
    """Test listing all registerd users."""
    # -- Empty listing --------------------------------------------------------
    cmd = ['users', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'alice' not in result.output
    # -- Register user Alice --------------------------------------------------
    cmd = ['users', 'register', '-u', 'alice', '-p', 'abc123']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # -- Listing contains alice now -------------------------------------------
    cmd = ['users', 'list']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    assert 'alice' in result.output
