# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file store functionality of a workflow group handle."""

import os
import pytest

from flowserv.model.files.fs import FileSystemStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.tests.files import FakeStream

import flowserv.error as err
import flowserv.tests.model as model
import flowserv.util as util


def test_delete_file(database, tmpdir):
    """Test deleting an uploaded file."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    file = FakeStream(data={'A': 1}).save()
    fn = 'data.json'
    fs = FileSystemStore(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_1 = model.create_group(session, workflow_id, users=[user_1])
        group_2 = model.create_group(session, workflow_id, users=[user_1])
        manager = WorkflowGroupManager(session=session, fs=fs)
        fh = manager.upload_file(group_id=group_1, file=file, name=fn)
        file_1 = fh.file_id
        fh = manager.upload_file(group_id=group_2, file=file, name=fn)
        file_2 = fh.file_id
    # -- Test delete file -----------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        fh, filename = manager.get_file(group_id=group_1, file_id=file_1)
        assert os.path.isfile(filename)
        manager.delete_file(group_id=group_1, file_id=file_1)
        assert not os.path.isfile(filename)
        # File 1 can no longer be accessed while file 2 is still present.
        with pytest.raises(err.UnknownFileError):
            manager.get_file(group_id=group_1, file_id=file_1)
        fh, filename = manager.get_file(group_id=group_2, file_id=file_2)
        assert os.path.isfile(filename)
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        # - Delete unknown file
        manager = WorkflowGroupManager(session=session, fs=fs)
        with pytest.raises(err.UnknownFileError):
            manager.delete_file(group_id=group_1, file_id=file_1)


def test_get_file(database, tmpdir):
    """Test accessing uploaded files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    data_1 = {'A': 1}
    data_2 = {'B': 2}
    f1 = FakeStream(data=data_1).save()
    f2 = FakeStream(data=data_2).save()
    fn = 'data.json'
    fs = FileSystemStore(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_1 = model.create_group(session, workflow_id, users=[user_1])
        group_2 = model.create_group(session, workflow_id, users=[user_1])
        mngr = WorkflowGroupManager(session=session, fs=fs)
        file_1 = mngr.upload_file(group_id=group_1, file=f1, name=fn).file_id
        file_2 = mngr.upload_file(group_id=group_2, file=f2, name=fn).file_id
    files = [(group_1, file_1, data_1), (group_2, file_2, data_2)]
    # -- Test get file --------------------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        for g_id, f_id, data in files:
            fh, filename = manager.get_file(group_id=g_id, file_id=f_id)
            assert fh.name == fn
            assert fh.mime_type == 'application/json'
            assert os.path.isfile(filename)
            assert util.read_object(filename=filename) == data
    # -- Test error cases -----------------------------------------------------
    # - File handle is unknown for s2
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        with pytest.raises(err.UnknownFileError):
            manager.get_file(group_id=group_2, file_id=file_1)
        # - Access file with unknown file identifier
        with pytest.raises(err.UnknownFileError):
            manager.get_file(group_id=group_1, file_id='UNK')


def test_list_files(database, tmpdir):
    """Test listing uploaded files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. The first group
    # has one uploaded file and the second group has one file.
    file = FakeStream(data={'A': 1}).save()
    fn = 'data.json'
    fs = FileSystemStore(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_1 = model.create_group(session, workflow_id, users=[user_1])
        group_2 = model.create_group(session, workflow_id, users=[user_1])
        manager = WorkflowGroupManager(session=session, fs=fs)
        manager.upload_file(group_id=group_1, file=file, name=fn)
        manager.upload_file(group_id=group_1, file=file, name=fn)
        manager.upload_file(group_id=group_2, file=file, name=fn)
    # -- Test list files for groups -------------------------------------------
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        files = manager.list_files(group_id=group_1)
        assert len(files) == 2
        files = manager.list_files(group_id=group_2)
        assert len(files) == 1


def test_upload_file(database, tmpdir):
    """Test uploading files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    fs = FileSystemStore(tmpdir)
    with database.session() as session:
        user_1 = model.create_user(session, active=True)
        workflow_id = model.create_workflow(session)
        group_1 = model.create_group(session, workflow_id, users=[user_1])
    # -- Test upload file -----------------------------------------------------
    data = {'A': 1}
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        fh = manager.upload_file(
            group_id=group_1,
            file=FakeStream(data={'A': 1}).save(),
            name='A.json'
        )
        assert fh.name == 'A.json'
        assert fh.mime_type == 'application/json'
        fh, filename = manager.get_file(group_id=group_1, file_id=fh.file_id)
        assert os.path.isfile(filename)
        assert util.read_object(filename=filename) == data
    # -- Test error case ------------------------------------------------------
    data = {'A': 1}
    with database.session() as session:
        with pytest.raises(err.ConstraintViolationError):
            manager.upload_file(
                group_id=group_1,
                file=FakeStream(data={'A': 1}).save(),
                name=' '
            )
        with pytest.raises(err.UnknownWorkflowGroupError):
            manager.upload_file(
                group_id='UNKNOWN',
                file=FakeStream(data={'A': 1}).save(),
                name=' '
            )
