# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the local workflow group service API."""

import pytest

from flowserv.tests.service import create_user

import flowserv.error as err
import flowserv.tests.serialize as serialize


@pytest.mark.parametrize('group_id', [None, 'G001'])
def test_create_group_view(group_id, local_service, hello_world):
    """Test serialization for created workflows."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one user and one instance of the 'Hello World' workflow.
    with local_service() as api:
        user_1 = create_user(api)
        wf = hello_world(api, name='W1')
        workflow_id = wf.workflow_id
    # Create a new workflow group with single user ----------------------------
    with local_service(user_id=user_1) as api:
        r = api.groups().create_group(
            workflow_id=workflow_id,
            name='G1',
            identifier=group_id
        )
        serialize.validate_group_handle(r)
        assert len(r['parameters']) == 3
        assert len(r['members']) == 1
    # Error when attempting to create a user without being authenticated.
    with local_service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.groups().create_group(workflow_id=workflow_id, name='G2')


def test_delete_group_view(local_service, hello_world):
    """Test deleting workflow groups via the API service."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two groups for the 'Hello World' workflow.
    with local_service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        wf = hello_world(api, name='W1')
        workflow_id = wf.workflow_id
    with local_service(user_id=user_1) as api:
        r = api.groups().create_group(
            workflow_id=workflow_id,
            name='G1'
        )
        group_id = r['id']
        api.groups().create_group(
            workflow_id=workflow_id,
            name='G2'
        )
    # -- User 2 cannot delete the first group ---------------------------------
    with local_service(user_id=user_2) as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.groups().delete_group(group_id=group_id)
    # -- Delete the first group -----------------------------------------------
    with local_service(user_id=user_1) as api:
        api.groups().delete_group(group_id=group_id)
        # After deleting one group the other group is still there.
        r = api.groups().list_groups(workflow_id=workflow_id)
        assert len(r['groups']) == 1
    # -- Error when deleting an unknown group ---------------------------------
    with local_service(user_id=user_1) as api:
        with pytest.raises(err.UnknownWorkflowGroupError):
            api.groups().delete_group(group_id=group_id)


def test_get_group_view(local_service, hello_world):
    """Create workflow group and validate the returned handle when retrieving
    the group view the service. In addition to the create_group test, this test
    creates a group with more than one member and additional workflow
    parameters.
    """
    # -- Setup ----------------------------------------------------------------
    #
    # Create two users and one instance of the 'Hello World' workflow.
    with local_service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        wf = hello_world(api, name='W1')
        workflow_id = wf.workflow_id
    # -- Create group with two members ----------------------------------------
    with local_service(user_id=user_1) as api:
        r = api.groups().create_group(
            workflow_id=workflow_id,
            name='G2',
            members=[user_2],
        )
        serialize.validate_group_handle(r)
        assert len(r['parameters']) == 3
        assert len(r['members']) == 2
    with local_service() as api:
        r = api.groups().get_group(r['id'])
        serialize.validate_group_handle(r)
        assert len(r['parameters']) == 3
        assert len(r['members']) == 2


def test_list_groups_view(local_service, hello_world):
    """Test serialization for group listings."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create two groups for the 'Hello World' workflow. The first group has one
    # member and the second group has two memebers.
    with local_service() as api:
        user_1 = create_user(api)
        user_2 = create_user(api)
        wf = hello_world(api, name='W1')
        workflow_id = wf.workflow_id
    with local_service(user_id=user_1) as api:
        api.groups().create_group(
            workflow_id=workflow_id,
            name='G1'
        )
        api.groups().create_group(
            workflow_id=workflow_id,
            name='G2',
            members=[user_1, user_2]
        )
    # -- Get group listing listing for workflow -------------------------------
    with local_service() as api:
        r = api.groups().list_groups(workflow_id=workflow_id)
        serialize.validate_group_listing(r)
        assert len(r['groups']) == 2
    # -- Get groups for user 1 and 2 separately -------------------------------
    with local_service(user_id=user_1) as api:
        r = api.groups().list_groups()
        assert len(r['groups']) == 2
    with local_service(user_id=user_2) as api:
        r = api.groups().list_groups()
        assert len(r['groups']) == 1


def test_update_group_view(local_service, hello_world):
    """Test updating group properties via the API service."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one group with minimal metadata for the 'Hello World' workflow.
    with local_service() as api:
        user_id = create_user(api)
        wf = hello_world(api, name='W1')
        workflow_id = wf.workflow_id
    with local_service(user_id=user_id) as api:
        r = api.groups().create_group(
            workflow_id=workflow_id,
            name='G1'
        )
        group_id = r['id']
    # -- Update group name ----------------------------------------------------
    with local_service(user_id=user_id) as api:
        r = api.groups().update_group(
            group_id=group_id,
            name='ABC'
        )
        assert r['name'] == 'ABC'
    with local_service() as api:
        # Update persists when retrieving the group handle.
        r = api.groups().get_group(group_id)
        assert r['name'] == 'ABC'
