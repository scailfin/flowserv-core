# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for uploaded files that are associated with workflow groups."""

import pytest

from flowserv.tests.files import FakeStream
from flowserv.tests.service import create_group, create_user

import flowserv.error as err
import flowserv.tests.serialize as serialize


def test_workflow_group_file_upload(api_factory, hello_world):
    """Test uploading files for a workflow group."""
    # Initialize the API
    api = api_factory()
    # Create two users.
    user_1 = create_user(api)
    user_2 = create_user(api)
    # Create a new workflows with ome group that has user 1 as the only member.
    workflow_id = hello_world(api, name='W1')['id']
    group_id = create_group(api, workflow_id=workflow_id, users=[user_1])
    # Upload first file for the group.
    r = api.uploads().upload_file(
        group_id=group_id,
        file=FakeStream(data={'group': 1, 'file': 1}),
        name='group1.json',
        user_id=user_1
    )
    serialize.validate_file_handle(r)
    assert r['name'] == 'group1.json'
    # Get serialized handle for the file and the group.
    file_id = r['id']
    fh, r = api.uploads().get_file(
        group_id=group_id,
        file_id=file_id,
        user_id=user_1
    )
    assert r['name'] == 'group1.json'
    assert fh.name == 'group1.json'
    gh = api.groups().get_group(group_id=group_id)
    serialize.validate_group_handle(gh)
    # Error trying to access file as non-member
    with pytest.raises(err.UnauthorizedAccessError):
        api.uploads().get_file(
            group_id=group_id,
            file_id=file_id,
            user_id=user_2
        )
