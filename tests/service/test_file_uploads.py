# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit test for uploaded files that are associated with workflow groups."""

from flowserv.tests.files import FakeStream
from flowserv.tests.service import create_group, create_user

import flowserv.tests.serialize as serialize


def test_group_file_upload_view(service, hello_world):
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
    with service() as api:
        fh, r = api.uploads().get_file(
            group_id=group_id,
            file_id=file_id,
            user_id=user_id
        )
        assert r['name'] == 'group1.json'
        assert fh.name == 'group1.json'
        gh = api.groups().get_group(group_id=group_id)
        serialize.validate_group_handle(gh)
