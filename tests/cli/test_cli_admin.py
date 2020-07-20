# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the command-line interface."""

import os
import pytest

from click.testing import CliRunner

from flowserv.cli.admin import cli
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.model.database import TEST_URL


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


def test_basedir_argument(tmpdir):
    """Test passing the base directory as an argument."""
    os.environ[FLOWSERV_DB] = TEST_URL
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-f', '-d', str(tmpdir)])
    assert result.exit_code == 0
    del os.environ[FLOWSERV_DB]


@pytest.mark.parametrize(
    'option',
    [None, '--all', '--database', '--auth', '--backend', '--service']
)
def test_config_options(cli_runner, option):
    """Test different options for the config command."""
    if option is None:
        result = cli_runner.invoke(cli, ['config'])
    else:
        result = cli_runner.invoke(cli, ['config', option])
    assert result.exit_code == 0


def test_create_basedir(tmpdir):
    """Test creating a non-existing API base directory."""
    basedir = os.path.join(tmpdir, 'api')
    os.environ[FLOWSERV_API_BASEDIR] = basedir
    os.environ[FLOWSERV_DB] = TEST_URL
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
    result = runner.invoke(cli, ['config', '--database'])
    assert 'export FLOWSERV_DATABASE=None' in result.output
    assert result.exit_code == 0


def test_init_without_force(tmpdir):
    """Test init without force option. Will terminate after printing confirm
    message.
    """
    os.environ[FLOWSERV_DB] = TEST_URL
    runner = CliRunner()
    result = runner.invoke(cli, ['init', '-d', str(tmpdir)])
    assert 'This will erase an existing database.' in result.output
    assert result.exit_code == 0
    del os.environ[FLOWSERV_DB]
