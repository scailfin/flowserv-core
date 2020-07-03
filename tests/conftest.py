
# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures for unit tests."""

import os
import pytest

from flowserv.model.db import DB, TEST_URL
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.model.workflow.manager import WorkflowManager

import flowserv.util as util


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=True)
    db.init()
    return db


@pytest.fixture
def wfmanager(database, tmpdir):
    """Create empty database. Return a test instance of the workflow
    repository manager.
    """
    repodir = util.create_dir(os.path.join(tmpdir, 'workflows'))
    return WorkflowManager(
        db=database,
        fs=WorkflowFileSystem(repodir)
    )
