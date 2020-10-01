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

from flowserv.cli.admin import cli
from flowserv.config.api import FLOWSERV_API_BASEDIR
from flowserv.config.database import FLOWSERV_DB
from flowserv.config.backend import CLEAR_BACKEND
from flowserv.model.base import WorkflowHandle
from flowserv.service.api import service


@pytest.fixture
def flowserv_cli(tmpdir):
    """Initialize the database and the API base directory for the flowserv
    command-line interface.
    """
    basedir = os.path.abspath(str(tmpdir))
    runner = CliRunner()
    os.environ[FLOWSERV_API_BASEDIR] = basedir
    os.environ[FLOWSERV_DB] = 'sqlite:///{}/flowserv.db'.format(basedir)
    CLEAR_BACKEND()
    runner = CliRunner()
    runner.invoke(cli, ['init', '-f'])
    with service() as api:
        api.engine.fs = api.fs
    yield runner
    # Using init does not seem to remove workflows from previous test runs.
    # Need to delete them here explicitly:
    from flowserv.service.database import database as db
    with db.session() as session:
        session.query(WorkflowHandle).delete()
    # Clear environment variables that were set for the test runner.
    del os.environ[FLOWSERV_API_BASEDIR]
    del os.environ[FLOWSERV_DB]
