# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) [2019-2020] NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow group service API."""

import os
import pytest

from flowserv.service.api import API
from flowserv.tests.controller import StateEngine
from flowserv.tests.parameter import StringParameter

import flowserv.tests.db as db

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.tests.serialize as serialize


DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_DIR = os.path.join(DIR, '../.files/benchmark/helloworld')

# Default users
USER_1 = util.get_unique_identifier()
USER_2 = util.get_unique_identifier()


def test_group_view(tmpdir):
    """Test serialization for created workflows groups and group listings."""
    # Get an API instance that uses the StateEngine as the backend
    con = db.init_db(str(tmpdir), users=[USER_1, USER_2]).connect()
    engine = StateEngine()
    api = API(con=con, engine=engine, basedir=str(tmpdir))
    # Create a new workflow group
    r = api.workflows().create_workflow(name='W1', sourcedir=TEMPLATE_DIR)
    workflow_id = r['id']
    # Create a new group for the workflow
    r = api.groups().create_group(
        workflow_id=workflow_id,
        name='G1',
        user_id=USER_1
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
        user_id=USER_1,
        members=[USER_2],
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
    r = api.groups().list_groups(user_id=USER_1)
    assert len(r['groups']) == 2
    r = api.groups().list_groups(user_id=USER_2)
    assert len(r['groups']) == 1
    # Update group name for G1 by user 2 will fail at first but succeed once
    # the user is a member of the group
    with pytest.raises(err.UnauthorizedAccessError):
        api.groups().update_group(group_id=g1, user_id=USER_2, name='ABC')
    api.groups().update_group(
        group_id=g1,
        user_id=USER_1,
        members=[USER_1, USER_2]
    )
    r = api.groups().update_group(group_id=g1, user_id=USER_2, name='ABC')
    assert r['name'] == 'ABC'
    r = api.groups().get_group(g1)
    assert r['name'] == 'ABC'
    assert len(r['members']) == 2
    # Delete the first group
    api.groups().delete_group(group_id=g1, user_id=USER_1)
    r = api.groups().list_groups(workflow_id=workflow_id)
    assert len(r['groups']) == 1
