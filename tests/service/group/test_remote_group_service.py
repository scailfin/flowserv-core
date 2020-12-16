# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the remote workflow group service API."""

from flowserv.model.parameter.numeric import Int


def test_create_group_remote(remote_service, mock_response):
    """Test creating a workflow group at the remote service."""
    remote_service.groups().create_group(workflow_id='0000', name='A')
    remote_service.groups().create_group(
        workflow_id='0000',
        name='A',
        members=['A', 'B'],
        parameters=[Int('a')]
    )


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
