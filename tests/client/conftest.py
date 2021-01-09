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

from flowserv.client.api import service
from flowserv.client.cli.base import cli
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.backend import CLEAR_BACKEND


@pytest.fixture
def flowserv_cli(tmpdir):
    """Initialize the database and the API base directory for the flowserv
    command-line interface.
    """
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    os.environ[FLOWSERV_API_BASEDIR] = basedir
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(basedir)
    # Make sure to reset the database.
    from flowserv.service.database import database
    database.__init__()
    CLEAR_BACKEND()
    runner = CliRunner()
    runner.invoke(cli, ['init', '-f'])
    with service() as api:
        api.fs = api.workflows().workflow_repo.fs
    yield runner
    # Clear environment variables that were set for the test runner.
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_DB]