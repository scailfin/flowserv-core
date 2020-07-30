# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the global workflow repository."""

import pytest

from flowserv.model.workflow.repository import WorkflowRepository


"""Test list of repository entries."""
TEMPLATES = [
    {'id': '0000', 'description': 'TEMPLATE_0', 'url': 'http://template.0'},
    {'id': '0001', 'description': 'TEMPLATE_1', 'url': 'http://template.1'}
]


@pytest.fixture
def repository():
    return WorkflowRepository(templates=TEMPLATES)


def test_workflow_repository_get(repository):
    """Test the get() method of the workflow repository."""
    assert repository.get('0000') == 'http://template.0'
    assert repository.get('0001') == 'http://template.1'
    assert repository.get('0002') == '0002'


def test_workflow_repository_list(repository):
    """Test the list() method of the workflow repository."""
    identifiers = [id for id, _, _ in repository.list()]
    assert identifiers == ['0000', '0001']
    descriptions = [desc for _, desc, _ in repository.list()]
    assert descriptions == ['TEMPLATE_0', 'TEMPLATE_1']
    urls = [url for _, _, url in repository.list()]
    assert urls == ['http://template.0', 'http://template.1']


def test_workflow_repository_load():
    """Ensure that the repository is loaded from the global URL if not
    templates are provided. This test doesn't make any assumptions about the
    content of the repository.
    """
    repository = WorkflowRepository()
    assert repository.list() is not None
