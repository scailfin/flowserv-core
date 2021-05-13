# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the user resources view."""

from flowserv.model.files import io_file
from flowserv.model.group import WorkflowGroupManager
from flowserv.view.files import UploadFileSerializer
from flowserv.view.validate import validator
from flowserv.volume.fs import FileSystemStorage

import flowserv.tests.model as model
import flowserv.view.files as labels


def test_file_listing_serialization(database, tmpdir):
    """Test serialization of file handles."""
    view = UploadFileSerializer()
    filename = 'data.json'
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=FileSystemStorage(basedir=tmpdir))
        user_id = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_id = model.create_group(session, workflow_id, users=[user_id])
        fh = manager.upload_file(
            group_id=group_id,
            file=io_file(data={'A': 1}),
            name=filename
        )
        doc = view.file_handle(group_id=group_id, fh=fh)
        assert doc[labels.FILE_NAME] == filename
        validator('FileHandle').validate(doc)
        doc = view.file_listing(
            group_id=group_id,
            files=manager.list_uploaded_files(group_id=group_id)
        )
        validator('FileListing').validate(doc)
