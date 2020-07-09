# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow group service API."""

import pytest

from flowserv.tests.parameter import StringParameter
from flowserv.tests.service import create_user

import flowserv.error as err
import flowserv.tests.serialize as serialize


def test_group_view(api_factory, hello_world):
    """Test serialization for created workflows groups and group listings."""
    # Get an API instance that uses the StateEngine as the backend.
    api = api_factory()
    # Create two users.
    user_1 = create_user(api)
    user_2 = create_user(api)
    # Create a new workflow group
    r = hello_world(api, name='W1')
    workflow_id = r['id']
    # Create a new group for the workflow
    r = api.groups().create_group(
        workflow_id=workflow_id,
        name='G1',
        user_id=user_1
    )
    serialize.validate_group_handle(r)
    assert len(r['parameters']) == 3
    assert len(r['members']) == 1
    # Retrieve the workflow group handle from the service
    r = api.groups().get_group(r['id'])
    serialize.validate_group_handle(r)
    assert len(r['parameters']) == 3
    assert len(r['members']) == 1
    g1 = r['id']
    # Create second group
    # Create second group with two members
    r = api.groups().create_group(
        workflow_id=workflow_id,
        name='G2',
        user_id=user_1,
        members=[user_2],
        parameters={
            'A': StringParameter('A'),
            'B': StringParameter('B')
        }
    )
    serialize.validate_group_handle(r)
    assert len(r['parameters']) == 5
    assert len(r['members']) == 2
    r = api.groups().get_group(r['id'])
    serialize.validate_group_handle(r)
    assert len(r['parameters']) == 5
    assert len(r['members']) == 2
    # Get group listing listing for workflow
    r = api.groups().list_groups(workflow_id=workflow_id)
    serialize.validate_group_listing(r)
    assert len(r['groups']) == 2
    # Get groups for user 1 and 2
    r = api.groups().list_groups(user_id=user_1)
    assert len(r['groups']) == 2
    r = api.groups().list_groups(user_id=user_2)
    assert len(r['groups']) == 1
    # Update group name for G1 by user 2 will fail at first but succeed once
    # the user is a member of the group
    with pytest.raises(err.UnauthorizedAccessError):
        api.groups().update_group(group_id=g1, user_id=user_2, name='ABC')
    api.groups().update_group(
        group_id=g1,
        user_id=user_1,
        members=[user_1, user_2]
    )
    r = api.groups().update_group(group_id=g1, user_id=user_2, name='ABC')
    assert r['name'] == 'ABC'
    r = api.groups().get_group(g1)
    assert r['name'] == 'ABC'
    assert len(r['members']) == 2
    # Delete the first group
    api.groups().delete_group(group_id=g1, user_id=user_1)
    r = api.groups().list_groups(workflow_id=workflow_id)
    assert len(r['groups']) == 1
