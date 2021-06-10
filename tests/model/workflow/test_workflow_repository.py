# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the global workflow repository."""

import pytest
import requests

from flowserv.model.workflow.repository import WorkflowRepository


"""Test list of repository entries."""
TEMPLATES = [
    {'id': '0000', 'description': 'TEMPLATE_0', 'url': 'http://template.0'},
    {
        'id': '0001',
        'description': 'TEMPLATE_1',
        'url': 'http://template.1',
        'manifest': 'myfile.txt',
        'args': [{'key': 'single_branch', 'value': True}]
    }
]


@pytest.fixture
def repository():
    return WorkflowRepository(templates=TEMPLATES)


class MockResponse:
    """Mock response object for requests to download the repository index.
    Adopted from the online documentation at:
    https://docs.pytest.org/en/stable/monkeypatch.html
    """
    def __init__(self, url):
        """Here we ignore the URL."""
        pass

    def json(self):
        """Return the TEMPLATES document."""
        return TEMPLATES

    def raise_for_status(self):
        """Never raise an error for a failed requests."""
        pass


@pytest.fixture
def mock_response(monkeypatch):
    """Requests.get() mocked to return index document."""

    def mock_get(*args, **kwargs):
        return MockResponse(*args)

    monkeypatch.setattr(requests, "get", mock_get)


def test_workflow_repository_get(repository):
    """Test the get() method of the workflow repository."""
    assert repository.get('0000') == ('http://template.0', None, dict())
    assert repository.get('0001') == ('http://template.1', 'myfile.txt', {'single_branch': True})
    assert repository.get('0002') == ('0002', None, dict())


def test_workflow_repository_list(repository):
    """Test the list() method of the workflow repository."""
    identifiers = [id for id, _, _ in repository.list()]
    assert identifiers == ['0000', '0001']
    descriptions = [desc for _, desc, _ in repository.list()]
    assert descriptions == ['TEMPLATE_0', 'TEMPLATE_1']
    urls = [url for _, _, url in repository.list()]
    assert urls == ['http://template.0', 'http://template.1']


def test_workflow_repository_load(mock_response):
    """Ensure that the repository is loaded from the global URL if not
    templates are provided. This test doesn't make any assumptions about the
    content of the repository.
    """
    repository = WorkflowRepository()
    identifiers = [id for id, _, _ in repository.list()]
    assert identifiers == ['0000', '0001']
