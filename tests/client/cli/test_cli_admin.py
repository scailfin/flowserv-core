# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the command-line interface."""

from click.testing import CliRunner

from flowserv.client.cli.base import cli_flowserv as cli


def test_config_options(flowserv_cli):
    """Test different options for the config command."""
    result = flowserv_cli.invoke(cli, ['config'])
    assert result.exit_code == 0


def test_config_error():
    """Test error when database environment variable is not set."""
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-f'])
    assert result.exit_code == 1


def test_init_db(flowserv_cli):
    """Test passing the base directory as an argument."""
    result = flowserv_cli.invoke(cli, ['init', '-f'])
    assert result.exit_code == 0


def test_init_without_force(flowserv_cli):
    """Test init without force option. Will terminate after printing confirm
    message.
    """
    result = flowserv_cli.invoke(cli, ['init'])
    assert 'This will erase an existing database.' in result.output
    assert result.exit_code == 0


def test_list_repository(flowserv_cli):
    """Test listing the contents of the global repository."""
    result = flowserv_cli.invoke(cli, ['repo'])
    assert result.exit_code == 0


def test_register_user(flowserv_cli):
    """Test creating a new user."""
    cmd = ['users', 'register', '-u', 'alice', '-p', 'mypwd']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # Registering the same user twice will raise an error.
    result = flowserv_cli.invoke(cli, cmd)
    assert str(result.exception) == "duplicate user 'alice'"
