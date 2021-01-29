# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2021 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file store functionality of a workflow group handle."""

import json
import pytest

from flowserv.config import Config
from flowserv.model.files.fs import FileSystemStore
from flowserv.model.group import WorkflowGroupManager
from flowserv.tests.files import DiskStore, io_file

import flowserv.error as err
import flowserv.tests.model as model


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_delete_file(fscls, database, tmpdir):
    """Test deleting an uploaded file."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    file = io_file(data={'A': 1})
    fn = 'data.json'
    fs = fscls(env=Config().basedir(tmpdir))
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
        fh = manager.get_uploaded_file(group_id=group_1, file_id=file_1)
        manager.delete_file(group_id=group_1, file_id=file_1)
        # File 1 can no longer be accessed while file 2 is still present.
        with pytest.raises(err.UnknownFileError):
            manager.get_uploaded_file(group_id=group_1, file_id=file_1).open()
        fh = manager.get_uploaded_file(group_id=group_2, file_id=file_2)
    # -- Error cases ----------------------------------------------------------
    with database.session() as session:
        # - Delete unknown file
        manager = WorkflowGroupManager(session=session, fs=fs)
        with pytest.raises(err.UnknownFileError):
            manager.delete_file(group_id=group_1, file_id=file_1)


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_get_file(fscls, database, tmpdir):
    """Test accessing uploaded files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    data_1 = {'A': 1}
    data_2 = {'B': 2}
    f1 = io_file(data=data_1)
    f2 = io_file(data=data_2)
    fn = 'data.json'
    fs = fscls(env=Config().basedir(tmpdir))
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
            fh = manager.get_uploaded_file(group_id=g_id, file_id=f_id)
            assert fh.name == fn
            assert fh.mime_type == 'application/json'
            assert json.load(fh.open()) == data
    # -- Test error cases -----------------------------------------------------
    # - File handle is unknown for s2
    with database.session() as session:
        manager = WorkflowGroupManager(session=session, fs=fs)
        with pytest.raises(err.UnknownFileError):
            manager.get_uploaded_file(group_id=group_2, file_id=file_1).open()
        # - Access file with unknown file identifier
        with pytest.raises(err.UnknownFileError):
            manager.get_uploaded_file(group_id=group_1, file_id='UNK').open()


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_list_files(fscls, database, tmpdir):
    """Test listing uploaded files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. The first group
    # has one uploaded file and the second group has one file.
    file = io_file(data={'A': 1})
    fn = 'data.json'
    fs = fscls(env=Config().basedir(tmpdir))
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
        files = manager.list_uploaded_files(group_id=group_1)
        assert len(files) == 2
        files = manager.list_uploaded_files(group_id=group_2)
        assert len(files) == 1


@pytest.mark.parametrize('fscls', [FileSystemStore, DiskStore])
def test_upload_file(fscls, database, tmpdir):
    """Test uploading files."""
    # -- Setup ----------------------------------------------------------------
    #
    # Create a database with two groups for a single workflow. Upload one file
    # for each group.
    fs = fscls(env=Config().basedir(tmpdir))
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
            file=io_file(data={'A': 1}),
            name='A.json'
        )
        assert fh.name == 'A.json'
        assert fh.mime_type == 'application/json'
        fh = manager.get_uploaded_file(group_id=group_1, file_id=fh.file_id)
        assert json.load(fh.open()) == data
    # -- Test error case ------------------------------------------------------
    data = {'A': 1}
    with database.session() as session:
        with pytest.raises(err.ConstraintViolationError):
            manager.upload_file(
                group_id=group_1,
                file=io_file(data={'A': 1}),
                name=' '
            )
        with pytest.raises(err.UnknownWorkflowGroupError):
            manager.upload_file(
                group_id='UNKNOWN',
                file=io_file(data={'A': 1}),
                name=' '
            )
