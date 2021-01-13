# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Fixtures for testing the command-line interface."""

import os
import pytest

from click.testing import CliRunner

from flowserv.client.cli.base import cli
from flowserv.model.database import TEST_DB

import flowserv.config as config


@pytest.fixture
def flowserv_cli(tmpdir):
    """Initialize the database and the API base directory for the flowserv
    command-line interface.
    """
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    os.environ[config.FLOWSERV_API_BASEDIR] = basedir
    os.environ[config.FLOWSERV_DB] = TEST_DB(basedir)
    os.environ[config.FLOWSERV_AUTH] = config.AUTH_OPEN
    # Make sure to reset the database.
    runner = CliRunner()
    runner.invoke(cli, ['init', '-f'])
    yield runner
    # Clear environment variables that were set for the test runner.
    del os.environ[config.FLOWSERV_API_BASEDIR]
    del os.environ[config.FLOWSERV_DB]
    del os.environ[config.FLOWSERV_AUTH]
