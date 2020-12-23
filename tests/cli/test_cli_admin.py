# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the command-line interface."""

import os

from click.testing import CliRunner

from flowserv.client.cli.base import cli
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.model.database import TEST_URL


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_basedir_argument(tmpdir):
    """Test passing the base directory as an argument."""
    os.environ[FLOWSERV_DB] = TEST_URL
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    # Make sure to reset the database.
    from flowserv.service.database import database
    database.__init__()
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-f'])
    assert result.exit_code == 0
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]


def test_config_options(flowserv_cli):
    """Test different options for the config command."""
    result = flowserv_cli.invoke(cli, ['config'])
    assert result.exit_code == 0


def test_create_basedir(tmpdir):
    """Test creating a non-existing API base directory."""
    basedir = os.path.join(tmpdir, 'api')
    os.environ[FLOWSERV_API_BASEDIR] = basedir
    os.environ[FLOWSERV_DB] = TEST_URL
    from flowserv.service.database import database
    database.__init__()
    assert not os.path.isdir(basedir)
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-f'])
    assert result.exit_code == 0
    assert os.path.isdir(basedir)
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_DB]


def test_config_error():
    """Test error when database environment variable is not set."""
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-f'])
    assert result.exit_code == -1
    result = runner.invoke(cli, ['config'])
    assert 'export FLOWSERV_DATABASE=None' in result.output
    assert result.exit_code == 0


def test_init_without_force(tmpdir):
    """Test init without force option. Will terminate after printing confirm
    message.
    """
    os.environ[FLOWSERV_API_BASEDIR] = str(tmpdir)
    os.environ[FLOWSERV_DB] = TEST_URL
    from flowserv.service.database import database
    database.__init__()
    runner = CliRunner()
    result = runner.invoke(cli, ['init'])
    assert 'This will erase an existing database.' in result.output
    assert result.exit_code == 0
    del os.environ[FLOWSERV_DB]
    del os.environ[FLOWSERV_API_BASEDIR]


def test_list_repository():
    """Test listing the contents of the global repository."""
    runner = CliRunner()
    result = runner.invoke(cli, ['repo'])
    assert result.exit_code == 0


def test_register_user(flowserv_cli):
    """Test creating a new user."""
    cmd = ['users', 'register', '-u', 'alice', '-p', 'mypwd']
    result = flowserv_cli.invoke(cli, cmd)
    assert result.exit_code == 0
    # Registering the same user twice will raise an error.
    result = flowserv_cli.invoke(cli, cmd)
    assert str(result.exception) == "duplicate user 'alice'"
