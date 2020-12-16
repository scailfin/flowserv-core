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
import requests

from flowserv.model.database import DB, TEST_URL
from flowserv.model.files.fs import FileSystemStore
from flowserv.service.api import API
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.group.remote import RemoteWorkflowGroupService
from flowserv.service.local import service as localservice
from flowserv.service.user.remote import RemoteUserService
from flowserv.service.workflow.remote import RemoteWorkflowService
from flowserv.view.user import USER_TOKEN
from flowserv.tests.controller import StateEngine

import flowserv.util as util


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')


# -- Helper class and fixture for mocked requests -----------------------------

class MockResponse:
    """Mock response object for API requests. Adopted from the online documentation
    at: https://docs.pytest.org/en/stable/monkeypatch.html
    """
    def __init__(self, url, json=None, headers=None):
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


@pytest.fixture
def mock_response(monkeypatch):
    """Requests.get() mocked to return {'mock_key':'mock_response'}."""

    def mock_delete(*args, **kwargs):
        return MockResponse(*args)

    def mock_get(*args, **kwargs):
        return MockResponse(*args)

    def mock_post(*args, **kwargs):
        return MockResponse(*args, **kwargs)

    monkeypatch.setattr(requests, "delete", mock_delete)
    monkeypatch.setattr(requests, "get", mock_get)
    monkeypatch.setattr(requests, "post", mock_post)


@pytest.fixture
def database():
    """Create a fresh instance of the database."""
    db = DB(connect_url=TEST_URL, web_app=False)
    db.init()
    return db


@pytest.fixture
def local_service(database, tmpdir):
    """Factory pattern for service API objects."""
    def _api(engine=StateEngine(), auth=None):
        return localservice(
            db=database,
            engine=engine,
            fs=FileSystemStore(basedir=tmpdir),
            auth=auth
        )

    return _api


@pytest.fixture
def remote_service():
    """Get a test instance for the remote service API."""
    doc = ServiceDescriptor().to_dict()
    doc['url'] = 'test'
    service = ServiceDescriptor(doc)
    return API(
        service=service,
        workflow_service=RemoteWorkflowService(descriptor=service),
        group_service=RemoteWorkflowGroupService(descriptor=service),
        upload_service=None,
        run_service=None,
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
