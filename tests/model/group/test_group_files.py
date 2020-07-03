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

from flowserv.model.base import User, WorkflowHandle
from flowserv.model.group import WorkflowGroupManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.tests.files import FakeStream

import flowserv.error as err
import flowserv.util as util


def init(db, basedir):
    """Create a fresh database with one user and one workflow. For the
    workflow, two groups are created. Returns the group manager and the
    two group identifier.
    """
    # Register three new users.
    user = User(user_id='U0000', name='U0000', secret='U0000', active=True)
    db.session.add(user)
    workflow = WorkflowHandle(
        workflow_id='W0000',
        name='W0000',
        workflow_spec='{}'
    )
    db.session.add(workflow)
    db.session.commit()
    manager = WorkflowGroupManager(
        db=db,
        fs=WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    )
    g1 = manager.create_group(
        workflow_id=workflow.workflow_id,
        name='Group 1',
        user_id=user.user_id,
        parameters=dict(),
        workflow_spec=dict()
    )
    g2 = manager.create_group(
        workflow_id=workflow.workflow_id,
        name='Group 2',
        user_id=user.user_id,
        parameters=dict(),
        workflow_spec=dict()
    )
    return manager, g1.group_id, g2.group_id


def test_delete_file(database, tmpdir):
    """Test deleting an uploaded file."""
    # Get group manager and group identifier.
    manager, g1, g2 = init(database, tmpdir)
    # Upload one file for each group
    fh1 = manager.upload_file(
        group_id=g1,
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = manager.upload_file(
        group_id=g2,
        file=FakeStream(data={'B': 2}),
        name='A.json'
    )
    fh1 = manager.get_file(group_id=g1, file_id=fh1.identifier)
    assert os.path.isfile(fh1.path)
    fh2 = manager.get_file(group_id=g2, file_id=fh2.identifier)
    assert os.path.isfile(fh2.path)
    assert fh1.path != fh2.path
    # Deleting a submission will delete all associated files
    manager.delete_file(group_id=g1, file_id=fh1.identifier)
    assert not os.path.isfile(fh1.path)
    assert os.path.isfile(fh2.path)
    # Error cases
    # - Delete unknown file
    with pytest.raises(err.UnknownFileError):
        manager.delete_file(group_id=g1, file_id=fh1.identifier)


def test_get_file(database, tmpdir):
    """Test accessing uploaded files."""
    # Get group manager and group identifier.
    manager, g1, g2 = init(database, tmpdir)
    # Upload one file for each group
    fh1 = manager.upload_file(
        group_id=g1,
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = manager.upload_file(
        group_id=g2,
        file=FakeStream(data={'B': 2}),
        name='A.txt'
    )
    fh1 = manager.get_file(group_id=g1, file_id=fh1.identifier)
    fh2 = manager.get_file(group_id=g2, file_id=fh2.identifier)
    assert fh1.name == 'A.json'
    assert fh1.mimetype == 'application/json'
    assert os.path.isfile(fh1.path)
    assert fh2.name == 'A.txt'
    assert fh2.mimetype == 'text/plain'
    assert os.path.isfile(fh2.path)
    # Check file contents
    assert util.read_object(filename=fh1.filename) == {'A': 1}
    assert util.read_object(filename=fh2.filename) == {'B': 2}
    # Error situations
    # - File handle is unknown for s2
    with pytest.raises(err.UnknownFileError):
        manager.get_file(group_id=g2, file_id=fh1.identifier)
    # - Access file with unknown file identifier
    with pytest.raises(err.UnknownFileError):
        manager.get_file(group_id=g1, file_id='UNK')


def test_list_files(database, tmpdir):
    """Test listing uploaded files."""
    # Get group manager and group identifier.
    manager, g1, g2 = init(database, tmpdir)
    # Upload two files gor group 1 and one file for group 2
    manager.upload_file(
        group_id=g1,
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    manager.upload_file(
        group_id=g1,
        file=FakeStream(data={'B': 2}),
        name='B.json'
    )
    fh3 = manager.upload_file(
        group_id=g2,
        file=FakeStream(data={'C': 3}),
        name='A.json'
    )
    # The file listing for group 1 should contain two files A.json and B.json
    files = manager.list_files(group_id=g1)
    assert len(files) == 2
    names = [f.name for f in files]
    assert 'A.json' in names
    assert 'B.json' in names
    # The file listing for group 2 should only contain A.json
    files = manager.list_files(group_id=g2)
    assert len(files) == 1
    fh = files[0]
    assert fh.name == 'A.json'
    assert fh.identifier == fh3.identifier


def test_upload_file(database, tmpdir):
    """Test uploading files."""
    # Get group manager and group identifier.
    manager, g1, g2 = init(database, tmpdir)
    # Upload one file for each group
    fh1 = manager.upload_file(
        group_id=g1,
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = manager.upload_file(
        group_id=g2,
        file=FakeStream(data={'B': 2}),
        name='A.txt'
    )
    assert fh1.name == 'A.json'
    assert fh1.mimetype == 'application/json'
    assert os.path.isfile(fh1.path)
    assert fh2.name == 'A.txt'
    assert fh2.mimetype == 'text/plain'
    assert os.path.isfile(fh2.path)
    assert util.read_object(filename=fh1.path) == {'A': 1}
    assert util.read_object(filename=fh2.path) == {'B': 2}
    # Test error cases
    # - Invalid file name
    with pytest.raises(err.ConstraintViolationError):
        manager.upload_file(
            group_id=g1,
            file=FakeStream(data={'A': 1}),
            name=' '
        )
