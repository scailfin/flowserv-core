# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Pytest fixtures for unit tests."""

import os
import pytest
import requests

from flowserv.config import Config
from flowserv.model.database import DB, TEST_URL
from flowserv.service.api import API
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.files.remote import RemoteUploadFileService
from flowserv.service.group.remote import RemoteWorkflowGroupService
from flowserv.service.local import LocalAPIFactory
from flowserv.service.run.remote import RemoteRunService
from flowserv.service.user.remote import RemoteUserService
from flowserv.service.workflow.remote import RemoteWorkflowService
from flowserv.view.user import USER_TOKEN
from flowserv.tests.controller import StateEngine

import flowserv.config as config
import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


# -- Helper class and fixture for mocked requests -----------------------------

class MockResponse:
    """Mock response object for API requests. Adopted from the online documentation
    at: https://docs.pytest.org/en/stable/monkeypatch.html
    """
    def __init__(self, url, files=None, json=None, headers=None):
        """Keep track of the request Url, and the optional request body and
        headers.
        """
        self.body = dict()
        if url == 'test/users/login':
            # Add user token to simulate successful login.
            self.body[USER_TOKEN] = '0000'

    def json(self):
        """Return dictionary containing the request Url and optional data. If
        the request.
        """
        return self.body

    def raise_for_status(self):
        """Never raise error for failed requests."""
        pass

    @property
    def raw(self):
        """Raw response for file downloads."""
        return None


@pytest.fixture
def mock_response(monkeypatch):
    """Requests.get() mocked to return {'mock_key':'mock_response'}."""

    def mock_get(*args, **kwargs):
        return MockResponse(*args)

    def mock_post(*args, **kwargs):
        return MockResponse(*args, **kwargs)

    monkeypatch.setattr(requests, "delete", mock_get)
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)
    monkeypatch.setattr(requests, "put", mock_post)


# -- Service API --------------------------------------------------------------

@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db


@pytest.fixture
def local_service(database, tmpdir):
    """Create a local API factory for test purposes."""
    env = Config().basedir(tmpdir).auth()
    return LocalAPIFactory(env=env, db=database, engine=StateEngine())


@pytest.fixture
def remote_service():
    """Get a test instance for the remote service API."""
    doc = ServiceDescriptor.from_config(env=config.env()).to_dict()
    doc['url'] = 'test'
    service = ServiceDescriptor(doc)
    return API(
        service=service,
        workflow_service=RemoteWorkflowService(descriptor=service),
        group_service=RemoteWorkflowGroupService(descriptor=service),
        upload_service=RemoteUploadFileService(descriptor=service),
        run_service=RemoteRunService(descriptor=service),
        user_service=RemoteUserService(descriptor=service)
    )


@pytest.fixture
def hello_world():
    """Factory pattern for Hello-World workflows. Assumes that we are in a
    local setting.
    """
    def _hello_world(api, name=None, description=None, instructions=None):
        return api.workflows().workflow_repo.create_workflow(
            name=name if name is not None else util.get_unique_identifier(),
            description=description,
            instructions=instructions,
            source=TEMPLATE_DIR
        )

    return _hello_world
