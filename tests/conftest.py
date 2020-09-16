
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

from flowserv.model.database import DB, TEST_URL
from flowserv.model.files.fs import FileSystemStore
from flowserv.service.api import service as serviceapi
from flowserv.tests.controller import StateEngine

import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, './.files/benchmark/helloworld')


@pytest.fixture
def service(database, tmpdir):
    """Factory pattern for service API objects."""
    def _api(engine=StateEngine(), auth=None, view=None):
        return serviceapi(
            db=database,
            engine=engine,
            fs=FileSystemStore(basedir=tmpdir),
            auth=auth,
            view=view
        )

    return _api


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db


@pytest.fixture
def hello_world():
    """Factory pattern for Hello-World workflows."""
    def _hello_world(api, name=None, description=None, instructions=None):
        return api.workflows().create_workflow(
            name=name if name is not None else util.get_unique_identifier(),
            description=description,
            instructions=instructions,
            source=TEMPLATE_DIR
        )

    return _hello_world
