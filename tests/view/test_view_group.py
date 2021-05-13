# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the workflow group resources view."""

from flowserv.model.files import io_file
from flowserv.model.group import WorkflowGroupManager
from flowserv.view.group import WorkflowGroupSerializer
from flowserv.view.validate import validator
from flowserv.volume.fs import FileSystemStorage

import flowserv.tests.model as model
import flowserv.view.group as labels


def test_group_handle_serialization(database, tmpdir):
    """Test serialization of workflow group handles."""
    view = WorkflowGroupSerializer()
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        manager.upload_file(group_id=group_id, file=io_file(data={'A': 1}), name='a.json')
        group = manager.get_group(group_id)
        doc = view.group_handle(group)
        validator('UserGroupHandle').validate(doc)
        assert len(doc[labels.GROUP_MEMBERS]) == 1


def test_group_listing_serialization(database, tmpdir):
    """Test serialization of workflow group listing."""
    view = WorkflowGroupSerializer()
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        model.create_group(session, workflow_id, users=[user_id])
        model.create_group(session, workflow_id, users=[user_id])
        groups = manager.list_groups(workflow_id=workflow_id, user_id=user_id)
        assert len(groups) == 2
        doc = view.group_listing(groups)
        validator('UserGroupListing').validate(doc)
        assert len(doc[labels.GROUP_LIST]) == 2
