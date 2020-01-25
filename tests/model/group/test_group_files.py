# This file is part of the Reproducible Open Benchmarks for Data Analysis
# Platform (ROB).
#
# Copyright (C) 2019 NYU.
#
# ROB is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Unit tests for the file store functionality of a workflow group handle."""

import os
import pytest

from flowserv.model.group.manager import WorkflowGroupManager
from flowserv.model.workflow.fs import WorkflowFileSystem
from flowserv.tests.files import FakeStream

import flowserv.core.error as err
import flowserv.core.util as util
import flowserv.tests.db as db


"""Unique identifier for users and workflow templates."""
USER_1 = '0000'
WORKFLOW_1 = '0000'


def init(basedir):
    """Create a fresh database with one user and one workflow. For the
    workflow, two groups are created and their group handles returned.
    """
    # Create new database with three users
    connector = db.init_db(
        str(basedir),
        workflows=[WORKFLOW_1],
        users=[USER_1]
    )
    con = connector.connect()
    manager = WorkflowGroupManager(
        con=con,
        fs=WorkflowFileSystem(os.path.join(str(basedir), 'workflows'))
    )
    g1 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 1',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    g2 = manager.create_group(
        workflow_id=WORKFLOW_1,
        name='Group 2',
        user_id=USER_1,
        parameters=dict(),
        workflow_spec=dict()
    )
    return g1, g2


def test_delete_file(tmpdir):
    """Test deleting an uploaded file."""
    # Get handles for workflow groups
    g1, g2 = init(tmpdir)
    # Upload one file for each group
    fh1 = g1.upload_file(
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = g2.upload_file(
        file=FakeStream(data={'B': 2}),
        name='A.json'
    )
    fh1 = g1.get_file(fh1.identifier)
    assert os.path.isfile(fh1.path)
    fh2 = g2.get_file(fh2.identifier)
    assert os.path.isfile(fh2.path)
    assert fh1.path != fh2.path
    # Deleting a submission will delete all associated files
    g1.delete_file(fh1.identifier)
    assert not os.path.isfile(fh1.path)
    assert os.path.isfile(fh2.path)
    # Error cases
    # - Delete unknown file
    with pytest.raises(err.UnknownFileError):
        g1.delete_file(fh1.identifier)


def test_get_file(tmpdir):
    """Test accessing uploaded files."""
    # Get handles for workflow groups
    g1, g2 = init(tmpdir)
    # Upload one file for each group
    fh1 = g1.upload_file(
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = g2.upload_file(
        file=FakeStream(data={'B': 2}),
        name='A.txt'
    )
    fh1 = g1.get_file(fh1.identifier)
    fh2 = g2.get_file(fh2.identifier)
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
        g2.get_file(fh1.identifier)
    # - Access file with unknown file identifier
    with pytest.raises(err.UnknownFileError):
        g1.get_file('UNK')


def test_list_files(tmpdir):
    """Test listing uploaded files."""
    # Get handles for workflow groups
    g1, g2 = init(tmpdir)
    # Upload two files gor group 1 and one file for group 2
    g1.upload_file(
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    g1.upload_file(
        file=FakeStream(data={'B': 2}),
        name='B.json'
    )
    fh3 = g2.upload_file(
        file=FakeStream(data={'C': 3}),
        name='A.json'
    )
    # The file listing for group 1 should contain two files A.json and B.json
    files = g1.list_files()
    assert len(files) == 2
    names = [f.name for f in files]
    assert 'A.json' in names
    assert 'B.json' in names
    # The file listing for group 2 should only contain A.json
    files = g2.list_files()
    assert len(files) == 1
    fh = files[0]
    assert fh.name == 'A.json'
    assert fh.identifier == fh3.identifier


def test_upload_file(tmpdir):
    """Test uploading files."""
    # Get handles for workflow groups
    g1, g2 = init(tmpdir)
    # Upload one file for each group
    fh1 = g1.upload_file(
        file=FakeStream(data={'A': 1}),
        name='A.json'
    )
    fh2 = g2.upload_file(
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
        g1.upload_file(
            file=FakeStream(data={'A': 1}),
            name=' '
        )
