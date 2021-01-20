# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote workflow group service API."""

import pytest

from flowserv.model.parameter.numeric import Int
from flowserv.service.descriptor import ServiceDescriptor
from flowserv.service.group.remote import RemoteWorkflowGroupService
from flowserv.view.group import GROUP_ID


@pytest.mark.parametrize('group_id', [None, 'G001'])
def test_create_group_remote(group_id, remote_service, mock_response):
    """Test creating a workflow group at the remote service."""
    # Minimal required parameters.
    remote_service.groups().create_group(workflow_id='0000', name='A')
    # All possible parameters.
    remote_service.groups().create_group(
        workflow_id='0000',
        name='A',
        members=['A', 'B'],
        parameters=[Int('a')],
        identifier=group_id
    )


def test_custom_group_labels():
    """Test initializing the remote group service with custom labels."""
    group_service = RemoteWorkflowGroupService(
        descriptor=ServiceDescriptor.from_config(env=dict()),
        labels={'GROUP_NAME': 'MY_NAME'}
    )
    assert group_service.labels['GROUP_ID'] == GROUP_ID
    assert group_service.labels['GROUP_NAME'] == 'MY_NAME'


def test_delete_group_remote(remote_service, mock_response):
    """Test deleting a workflow group at the remote service."""
    remote_service.groups().delete_group(group_id='0000')


def test_get_group_remote(remote_service, mock_response):
    """Test getting a workflow handle from the remote service."""
    remote_service.groups().get_group(group_id='0000')


def test_list_groups_remote(remote_service, mock_response):
    """Test getting a group listing from the remote service."""
    remote_service.groups().list_groups()
    remote_service.groups().list_groups(workflow_id='0000')


def test_update_group_remote(remote_service, mock_response):
    """Test updating a group at the remote service."""
    remote_service.groups().update_group(group_id='0000')
    remote_service.groups().update_group(group_id='0000', name='A', members=['A', 'B'])
