# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures for unit tests."""

import pytest

from flowserv.config import Config
from flowserv.model.database import DB, TEST_URL
from flowserv.service.local import LocalAPIFactory


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db


@pytest.fixture
def async_service(database, tmpdir):
    """Create a local API factory that executes workflows asynchronously."""
    env = Config().basedir(tmpdir).run_async().auth()
    return LocalAPIFactory(env=env)


@pytest.fixture
def sync_service(database, tmpdir):
    """Create a local API factory that executes workflows synchronously."""
    env = Config().basedir(tmpdir).run_sync().auth()
    return LocalAPIFactory(env=env, db=database)
