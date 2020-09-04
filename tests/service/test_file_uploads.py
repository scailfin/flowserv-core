# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for uploaded files that are associated with workflow groups."""

import os
import pytest

from flowserv.tests.files import FakeStream
from flowserv.tests.service import create_group, create_user, upload_file

import flowserv.error as err
import flowserv.tests.serialize as serialize


def test_delete_group_file_view(service, hello_world):
    """Test deleting an uploaded file for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Upload one file for a workflow group.
    with service() as api:
        user_id = create_user(api)
        r = hello_world(api, name='W1')
        workflow_id = r['id']
        group_id = create_group(api, workflow_id=workflow_id, users=[user_id])
        file_id = upload_file(
            api=api,
            group_id=group_id,
            file=FakeStream(data={'group': 1, 'file': 1}),
            user_id=user_id
        )
    # -- Error when unknown user attempts to delete the file ------------------
    with service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.uploads().delete_file(group_id, file_id, 'UNKNNOWN')
    # -- Delete the uploaded file ---------------------------------------------
    with service() as api:
        api.uploads().delete_file(group_id, file_id, user_id)
    # After deletion the file cannot be accessed anymore.
    with service() as api:
        with pytest.raises(err.UnknownFileError):
            api.uploads().get_file(group_id, file_id, user_id)


def test_list_group_files_view(service, hello_world):
    """Test getting a listing of uploaded files for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Upload two files for a workflow group.
    with service() as api:
        user_id = create_user(api)
        r = hello_world(api, name='W1')
        workflow_id = r['id']
        group_id = create_group(api, workflow_id=workflow_id, users=[user_id])
        for i in range(2):
            upload_file(
                api=api,
                group_id=group_id,
                file=FakeStream(data={'group': 1, 'file': i}),
                user_id=user_id
            )
    # -- Get file listing -----------------------------------------------------
    with service() as api:
        files = api.uploads().list_files(group_id=group_id, user_id=user_id)
        serialize.validate_file_listing(files, 2)
    # -- Error when listing files as unknonw user -----------------------------
    with service() as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.uploads().list_files(group_id=group_id, user_id='UNKNOWN')


def test_upload_group_file_view(service, hello_world):
    """Test uploading files for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one group with minimal metadata for the 'Hello World' workflow.
    with service() as api:
        user_id = create_user(api)
        r = hello_world(api, name='W1')
        workflow_id = r['id']
        group_id = create_group(api, workflow_id=workflow_id, users=[user_id])
    # -- Upload first file for the group --------------------------------------
    with service() as api:
        r = api.uploads().upload_file(
            group_id=group_id,
            file=FakeStream(data={'group': 1, 'file': 1}),
            name='group1.json',
            user_id=user_id
        )
        file_id = r['id']
        serialize.validate_file_handle(r)
        assert r['name'] == 'group1.json'
    # -- Get serialized handle for the file and the group ---------------------
    for uid in [user_id, None]:
        with service() as api:
            fh, filename = api.uploads().get_file(group_id, file_id, uid)
            assert fh.name == 'group1.json'
            assert os.path.exists(filename)
            gh = api.groups().get_group(group_id=group_id)
            serialize.validate_group_handle(gh)
