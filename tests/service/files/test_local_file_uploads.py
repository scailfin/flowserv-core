# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for file uploads using the local file service."""

import pytest

from flowserv.model.files import io_file
from flowserv.tests.service import create_group, create_user, upload_file

import flowserv.error as err
import flowserv.tests.serialize as serialize


def test_delete_group_file_local(local_service, hello_world):
    """Test deleting an uploaded file for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Upload one file for a workflow group.
    with local_service() as api:
        user_id = create_user(api)
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
    with local_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id=workflow_id)
        file_id = upload_file(
            api=api,
            group_id=group_id,
            file=io_file(data={'group': 1, 'file': 1})
        )
    # -- Error when unknown user attempts to delete the file ------------------
    with local_service(user_id='UNKNNOWN') as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.uploads().delete_file(group_id, file_id)
    # -- Delete the uploaded file ---------------------------------------------
    with local_service(user_id=user_id) as api:
        api.uploads().delete_file(group_id, file_id)
    # After deletion the file cannot be accessed anymore.
    with local_service(user_id=user_id) as api:
        with pytest.raises(err.UnknownFileError):
            api.uploads().get_uploaded_file(group_id, file_id)


def test_list_group_files_local(local_service, hello_world):
    """Test getting a listing of uploaded files for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Upload two files for a workflow group.
    with local_service() as api:
        user_id = create_user(api)
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
    with local_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id=workflow_id)
        for i in range(2):
            upload_file(
                api=api,
                group_id=group_id,
                file=io_file(data={'group': 1, 'file': i})
            )
    # -- Get file listing -----------------------------------------------------
    with local_service(user_id=user_id) as api:
        files = api.uploads().list_uploaded_files(
            group_id=group_id
        )
        serialize.validate_file_listing(files, 2)
    # -- Error when listing files as unknonw user -----------------------------
    with local_service(user_id='UNKNOWN') as api:
        with pytest.raises(err.UnauthorizedAccessError):
            api.uploads().list_uploaded_files(
                group_id=group_id
            )


def test_upload_group_file_local(local_service, hello_world):
    """Test uploading files for a workflow group."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create one group with minimal metadata for the 'Hello World' workflow.
    with local_service() as api:
        user_id = create_user(api)
        workflow = hello_world(api, name='W1')
        workflow_id = workflow.workflow_id
    with local_service(user_id=user_id) as api:
        group_id = create_group(api, workflow_id=workflow_id)
    # -- Upload first file for the group --------------------------------------
    with local_service(user_id=user_id) as api:
        r = api.uploads().upload_file(
            group_id=group_id,
            file=io_file(data={'group': 1, 'file': 1}),
            name='group1.json'
        )
        file_id = r['id']
        serialize.validate_file_handle(r)
        assert r['name'] == 'group1.json'
    # -- Get serialized handle for the file and the group ---------------------
    for uid in [user_id, None]:
        with local_service(user_id=uid) as api:
            fcont = api.uploads().get_uploaded_file(group_id, file_id).read()
            assert fcont == b'{"group": 1, "file": 1}'
            gh = api.groups().get_group(group_id=group_id)
            serialize.validate_group_handle(gh)
